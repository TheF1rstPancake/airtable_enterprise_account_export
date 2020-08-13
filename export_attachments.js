const axios = require('axios');
const Promise = require('bluebird');
const sqlite3 = require('sqlite3');
const airtable = require('airtable');
const config = require('./config');
const uuid = require('uuid');

const fs = require('fs');
const path = require('path');

const csv = require('fast-csv');

const ATTACHMENT_DIR = path.join('.', 'attachments');

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

function findAttachmentField(table) {
  return table.fields.filter((f) => {
    return f.type === 'multipleAttachments';
  });
}

async function downloadAttachmentsAsync(base_id, table, records, attachment_fields, attachment_path) {
  // for each record, pull out their attachments
  var num_attachments = 0;
  var attachments = [];
  for (let r of records) {
    for (let f of attachment_fields) {
      if (r.fields[f.name] !== undefined) {
        attachments = [...attachments, ...r.fields[f.name]];
      }
    }
  }

  // update our counter 
  num_attachments += attachments.length;
  console.log(`Found ${num_attachments} attachments in ${base_id}: ${table.id}`);
  console.log(`Downloading attachments to ${attachment_path}`);

  // for each attachment, make a request to pull down the attachment
  // and write it to this base's folder
  // let's also limit how many attachments we try and fetch at once
  await Promise.map(attachments, async (a) => {
    var response = await axios({
      url: a.url,
      method: 'GET',
      responseType: 'stream'
    });

    // the file path for any given file is our attachment
    // directory + (attachment ID_filename) 
    // using the ID ensures we don't overwrite files that may have the same name in a base
    var cleaned_name = a.filename.replace("?authuser=0", '');
    var file = path.join(attachment_path, `${a.id}_${cleaned_name}`);


    try {
      var writer = fs.createWriteStream(file);

      response.data.pipe(writer);
      var p = new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
      });
      await p;
    } catch (err) {
      console.log("ERR: could not write file to ", file);
      console.log(err);

    }
    return;
  }, {
    concurrency: 10
  });
  return num_attachments;
}


/**
 * Run this script to read all data out of all bases in the Enterprise Account
 * @param {string[]} bases
 */
async function run(bases) {

  // bases should be a list of objects
  // [{id: 'apXXXXX', ...}, ....]
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


    // create a folder for managing this base's attachments
    var attachment_path = path.join(ATTACHMENT_DIR, base_id);
    if (!fs.existsSync(attachment_path)) {
      fs.mkdirSync(attachment_path);
    }

    // iterate over each table.  Don't complete the operation until all records have been written to the database
    // the return will be a list of records written for each table.  We can sum those values to get the total number of records written
    let tables = metadata.data.tables;
    try {
      var record_numbers = await Promise.map(tables, async (t) => {
        // get all records.  This handles pagination for us
        console.log(`Pulling data from ${t.id} in ${base_id}`);
        var num_attachments = 0;

        var attachment_fields = findAttachmentField(t);
        console.log(`Found ${JSON.stringify(attachment_fields)} attachment fields`);
        if (attachment_fields.length === 0) {
          console.log(`Found 0 attachment fields in table -- ${table.id}`);
          return 0;
        }

        try {
          await requestWithRetry(
            base(t.name).select().eachPage(async function page(records, fetchNextPage) {
              // for each attachment, make a request to pull down the attachment
              // and write it to this base's folder
              var num_downloaded = await downloadAttachmentsAsync(
                base_id, t, records, attachment_fields, attachment_path
              );

              num_attachments += num_downloaded;

              fetchNextPage();
            })
          );
        } catch (err) {
          console.log(`ERR: Error requesting all data from base ${base_id} and table ${t.name}. Skipping this scan`);
          throw err;
        }

        console.log(`All attachments written for ${t.id} in ${base_id} -- ${num_attachments}`);
        return num_attachments;
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
    console.log(`${num_records} attachments scanned for ${base_id}`);

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
  }, {
    concurrency: 2
  });
}

// assumes that all database tables have already been created
if (require.main === module) {
  var tic = new Date();

  var p = new Promise((resolve, reject) => {
    let rows = []
    fs.createReadStream(path.join('.', 'listofbases.csv'))
      .pipe(csv.parse({
        headers: true
      }))
      .on('data', row => {
        rows.push(row);
      })
      .on('end', () => {
        resolve(rows);
      })
      .on('error', (err) => {
        reject(err);
      })
  });

  Promise.resolve(p)
    .then((bases) => {
      return run(bases)
    }).then((res) => {
      let toc = new Date();
      if (UNSCANNED_BASES.length !== 0) {
        console.log('WARN: could not scan all bases.  Here are the ones we had to skip');
        console.log(JSON.stringify(UNSCANNED_BASES));
      }
      console.log(`Success.  Attachments downloaded`);
      console.log(`Operation took ${(toc.getTime()-tic.getTime())/1000} seconds`);
    })
    .catch((err) => {
      let toc = new Date();
      console.log("ERR: ", err)
      console.log("Exiting: ", toc);
      return;
    });

}

module.exports = {
  run: run
}