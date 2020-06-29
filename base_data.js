const axios = require('axios');
const Promise = require('bluebird');
const sqlite3 = require('sqlite3');
const airtable = require('airtable');
const config = require('./config');
const uuid = require('uuid');

const argv = require('yargs')
  .scriptName("bulk-export")
  .argv;

var UNSCANNED_BASES = [];

/**
 * For each base:
 *  Add admin as a read only collaborator to a base
 *  Read all data out of the base
 *  Write records to local sqlite database
 *  Remove admins as collaborator
 */

const admin = axios.create({
  baseURL: 'https://api.airtable.com/v0/',
  headers: {
    'Authorization': `Bearer ${process.env.AIRTABLE_API_ADMIN_KEY}`
  }
});

// only retry a request 5 times before nuking it
async function requestWithRetry(request, request_id, retry_count) {
  retry_count = retry_count === undefined ? 0 : retry_count;
  try {
    let r = await request;
    return r;
  } catch (err) {
    if (err.code === 'ETIMEDOUT' && retry_count < 5) {
      console.log(`WARN: Received timeout error while accessing ${request_id}.  ${JSON.stringify(err)}. Sleeping and retrying`);
      retry_count += 1;
      let p = new Promise((resolve, reject) => {
        setTimeout(() => {
          console.log(`WARN: waking and retrying for ${request_id}`);
          requestWithRetry(request, request_id, retry_count).then((req) => {
            resolve(req);
          }).catch((err) => {
            reject(err);
          })

        }, 10000);
      })
      return await p;
    } else {
      if (retry_count >= 5) {
        console.log(`WARN: too many retries for ${request_id}`);
      }
      throw err;
    }
  }
}

/**
 * Run an operation in our SQLite database.  Returns a promise which resolves when the statement completes.
 * @param {*} db sqlite database connection object
 * @param {*} statement SQL statement (string) to run
 * @param {*} payload data to include in the SQL statement
 */
function asyncOp(db, statement, payload) {
  return new Promise((resolve, reject) => {
    db.run(statement, payload, (err, row) => {
      if (err !== null) {
        reject(err);
        return;
      }
      resolve(row);
    });
  });
}

async function deleteData(db, scan_id, table_name, primary_field) {
  // find all items in this table where the base was never scanned OR the scan_id does not
  // match the one provided. This is the list of bases that we did not scan
  // in this most recent run 
  let to_delete = await (new Promise((resolve, reject) => {
    db.all(`SELECT ${primary_field} FROM ${table_name} WHERE scan_id !=? OR scan_id IS NULL`, [scan_id], function (err, rows) {
      if (err !== null) {
        reject(err);
        return;
      }
      resolve(rows);
    })
  }));


  // build the list of ids that we need to delete
  console.log(`Found ${to_delete.length} items to delete`);
  to_delete = to_delete.map((b) => {
    return b[primary_field]
  })
  console.log(`Deleting ${to_delete} items`);

  let qs = to_delete.map(() => {
    return '?'
  }).join(",");
  await asyncOp(db, `DELETE FROM ${table_name} WHERE ${primary_field} IN(${qs})`, to_delete);
}


/**
 * Run this script to read all data out of all bases in the Enterprise Account
 * @param {*} db 
 * @param {string} scan_id 
 * @param {boolean} delete_data
 */
async function run(db, scan_id, delete_data, ) {

  // pull all bases out of our sqlite database
  console.log("Fetching : ", scan_id);

  let p = new Promise((resolve, reject) => {
    db.all('SELECT * FROM bases WHERE scan_id !=? OR scan_id IS NULL', [scan_id], function (err, rows) {
      if (err !== null) {
        reject(err);
        return;
      }
      resolve(rows);
    })
  });
  var bases = await p;
  console.log(`Found ${bases.length} bases`);

  // now for all bases, add the admin as a read only collaborator
  // then go through and download the data
  // for each table in the base
  // API rate limits are per base, so we can scan 5 tables in parallel in a given base
  // limiting the number of bases at a time to prevent overload on our machine.
  console.log(`Fetching data`);

  await Promise.map(bases, async (b) => {
    let base_id = b.id;
    console.log(`Adding admin to base ${base_id}`);
    try {
      var r = await requestWithRetry(admin.post(`meta/bases/${base_id}/collaborators`, {
        collaborators: [{
          user: {
            id: config.adminUserId
          },
          permissionLevel: 'read'
        }]
      }), base_id);
    } catch (err) {
      // it is possible the base no longer exists.  return 0 records in this case and do not update the scan ID
      if ((err.response.status === 404 || err.response.data.error === 'NOT_FOUND')) {
        console.log(`WARN: ${base_id} cannot be found.  Skipping scan`);
        return 0;
      } else if (err.response.status === 403) {
        console.log(`WARN: Admin could not be added to base_id ${base_id}.  Skipping`);
        UNSCANNED_BASES.push(base_id);
        return 0;
      }
      throw err;
    }


    // for each base, we need to get a list of all of it's tables via metadata api
    console.log(`Getting tables for base ${base_id}`);
    try {
      var metadata = await requestWithRetry(admin.get(`meta/bases/${base_id}/tables`), base_id);
    } catch (err) {
      if (err.response.status === 403) {
        console.log(`WARN: ${base_id} is not set up for Metadata API.  It may still be on a Pro Plan.  Reach out to Airtable Support for more details`);
        return 0;
      }
      throw err;
    }

    // set up the Airtable SDK to take action on the given base
    let base = new airtable({
      apiKey: process.env.AIRTABLE_API_ADMIN_KEY
    }).base(base_id);

    // iterate over each table.  Don't complete the operation until all records have been written to the database
    // the return will be a list of records written for each table.  We can sum those values to get the total number of records written
    let tables = metadata.data.tables;
    try {
      var record_numbers = await Promise.map(tables, async (t) => {
        // get all records.  This handles pagination for us
        console.log(`Pulling data from ${t.id} in ${base_id}`);
        try {
          var records = await requestWithRetry(base(t.name).select().all(), t.id);
        } catch (err) {
          console.log(`ERR: Error requesting all data from base ${base_id} and table ${t.name}. Skipping this scan`);
          throw err;
        }
        console.log(`Writing ${records.length} records to database for ${t.id} in ${base_id}`);

        // for each record, create the object to write to our database
        var promises = records.map((rec) => {
          let payload = {
            $base_id: base_id,
            $table_id: t.id,
            $record_id: rec.id,
            $data: JSON.stringify(rec.fields),
            $created_time: rec._rawJson.createdTime,
            $scan_id: scan_id
          }
          return asyncOp(db, `INSERT OR REPLACE INTO data (base_id, table_id, record_id, created_time, data, scan_id) VALUES ($base_id, $table_id, $record_id, $data, $created_time, $scan_id)`, payload);
        });

        // wait for all promises to resolve before moving on.
        let results = await Promise.all(promises);
        console.log(`All records written for ${t.id} in ${base_id}`);
        return results.length;
      }, {
        concurrency: 5
      });
    } catch (err) {
      // if a single table fails, the entire Promise.all() fails.  We do not want to kill the entire process
      // we should flag the base so that it can be scanned again in the future. 
      // this will skip the base from being written as "scanned"
      // but won't prevent us from trying to scan the rest of the bases
      console.log(`ERR: was not able to scan all of ${base_id}.  Skipping so other bases can be scanned`)
      console.log(err);
      UNSCANNED_BASES.push(base_id);
      return;
    }
    // calculate the total number of records.  This can be helpful for logging and debugging
    let num_records = record_numbers.reduce((aggregate, current) => {
      return aggregate + current;
    });
    console.log(`${num_records} records written for ${base_id}`);

    // remove the admin from the base and mark the base as scanned
    // there are cases where the remove op will fail.  If the admin is a workspace collaborator, adding them at the base level is a no-op
    // and then removing them requires doing so at the workspace level, but that may not be desirable
    // we should check for the 403 error here and then provide a warning message, but continue
    let now = new Date();
    console.log(`Cleaning up admin permissions and marking base as scanned ${now.toISOString()}`)
    try {
      await requestWithRetry(admin.delete(`meta/bases/${base_id}/collaborators/${config.adminUserId}`), base_id);
    } catch (err) {
      if (err.response.status === 403) {
        console.log(`WARN: Attempting to remove admin from base ${base_id} failed with ${err.response.status} and ${JSON.stringify(err.response.data)}.  The user was likely already a workspace collaborator.  Taking NO further action`);
      } else {
        throw err;
      }
    }

    // finally, update the bases scan time and scan id
    await asyncOp(db, `UPDATE bases SET scan_time=?, scan_id=? WHERE id=?;`, [now.toISOString(), scan_id, base_id]);
  }, {
    concurrency: 2
  });


  // after we've scanned all bases, check if we need to delete data
  if (delete_data !== true) {
    console.log("Delete data flag not passed.  Not deleting any bases or data");
    return;
  }
  console.log("Delete data flag passed.  Beginning to remove bases and data that no longer exist in Enterprise account");

  // delete bases
  await deleteData(db, scan_id, 'bases', 'id');

  // delete records
  await deleteData(db, scan_id, 'data', 'record_id');

}

// assumes that all database tables have already been created
if (require.main === module) {
  // create the scan ID for this scan
  var scan_id = uuid.v4();

  if (argv.scan_id !== undefined) {
    console.log("Using supplied scan ID: ", argv.scan_id);
    scan_id = argv.scan_id;
  }
  console.log("Scan ID: ", scan_id);

  var delete_data = false;
  if (argv.delete_data !== undefined) {
    delete_data = true;
  }
  console.log("delete_data flag provided.  Will remove data at end of script if applicable");


  var db = new sqlite3.Database('export.sqlite');
  var tic = new Date();
  run(db, scan_id, delete_data)
    .then((res) => {
      let toc = new Date();
      if (UNSCANNED_BASES.length !== 0) {
        console.log('WARN: could not scan all bases.  Here are the ones we had to skip');
        console.log(JSON.stringify(UNSCANNED_BASES));
      }
      console.log(`Success ${scan_id}.  Base data parsed`);
      console.log(`Operation took ${(toc.getTime()-tic.getTime())/1000} seconds`);
      db.close();
    })
    .catch((err) => {
      let toc = new Date();
      console.log("ERR: ", err)
      console.log("Exiting: ", toc);
      db.close();
      return;
    });

}

module.exports = {
  run: run
}