const axios = require('axios');
const Promise = require('bluebird');
const sqlite3 = require('sqlite3');
const config = require("./config");
/**
 * Find all workspaces from an Enterprise account
 * Find all bases in those workspaces
 * For each base:
 *  Add admin as a read only collaborator to a base
 *  Read all data out of the base
 *  Write records to local sqlite database
 *  Remove admins as collaborator
 * 
 * Create three tables:
 *  workspaces -- workspace id, name, owners and created time
 *  bases -- workspace id, base id, name, created time, scan time, scan_id
 *  data -- base id, table id, record id, json data, created time, scan_id
 */

;

const admin = axios.create({
  baseURL: 'https://api.airtable.com/v0/',
  headers: {
    'Authorization': `Bearer ${process.env.AIRTABLE_API_ADMIN_KEY}`
  }
});

async function run(db) {
  // get a list of all users and workspaces.  Do this for each enterprise account id
  for (var i in config.enterpriseAccountId) {
    let enterpriseAccountId = config.enterpriseAccountId[i];
    var res = await admin.get(`meta/enterpriseAccounts/${enterpriseAccountId}`);

    var user_ids = res.data.userIds;
    var workspaces = res.data.workspaceIds;


    // for each workspace, get a list of all bases
    // start building out our queue of bases that we need to scan
    // at this point, we can also get data about who owns the workspace
    var queue = [];
    console.log("Fetching workspace data and writing to database");
    await Promise.map(workspaces, async (wid) => {
      let r = await admin.get(`meta/workspaces/${wid}?include=collaborators`);

      let owners = r.data.collaborators.workspaceCollaborators.filter((o) => {
        if (o.permissionLevel === 'owner') {
          return o;
        }
      }).map(o => {
        return o.email
      }).join(',');

      // build the payload for what we will write to our local database about the workspace
      let payload = {
        $id: wid,
        $owners: owners,
        $created_time: r.data.created_time,
        $name: r.data.name
      };

      // wait for the record to be inserted
      new Promise((resolve, reject) => {
        db.run('INSERT OR REPLACE INTO workspaces (id, owners, created_time, name) VALUES ($id, $owners, $created_time, $name)', payload, ((err, rows) => {
          if (err !== null) {
            reject(err);
          }
          resolve(rows);
        }));
      });

      // iterate through all base IDs in the workspace and create the object
      // for our queue to scan
      for (var i in r.data.baseIds) {
        let b = r.data.baseIds[i];
        queue.push({
          workspace_id: wid,
          base_id: b
        });
      }
    }, {
      concurrency: 10
    });


    // now for each item in the queue
    // fetch the base and write the base details to the table
    console.log(`Found ${queue.length} bases`);
    console.log("Fetching bases and writing to database");
    await Promise.map(queue, async (q) => {
      let r = await admin.get(`meta/bases/${q.base_id}`);

      let payload = {
        $id: q.base_id,
        $workspace_id: q.workspace_id,
        $name: r.data.name,
        $created_time: r.data.created_time
      }

      // insert
      await db.run('INSERT OR REPLACE INTO bases (id, workspace_id, created_time, name) VALUES ($id, $workspace_id, $created_time, $name)', payload)

    }, {
      concurrency: 10
    })
  }

}

/**
 * Create the tables in our SQLite database
 * @param {*} db SQLite database object
 */
function createTables(db) {
  db.serialize(() => {
    db.run('CREATE TABLE IF NOT EXISTS workspaces (id TEXT PRIMARY KEY, owners TEXT, created_time TEXT, name TEXT);');

    db.run('CREATE TABLE IF NOT EXISTS bases (id TEXT PRIMARY KEY, workspace_id TEXT, name TEXT, created_time TEXT, scan_time TEXT, scan_id TEXT);');

    db.run('CREATE TABLE IF NOT EXISTS data (base_id TEXT, table_id TEXT, record_id TEXT PRIMARY KEY, data TEXT, created_time TEXT, scan_id TEXT, FOREIGN KEY(base_id) REFERENCES bases(id) ON DELETE CASCADE)')
  });

  return db;
}

if (require.main === module) {
  var db = new sqlite3.Database('export.sqlite');

  // create the tables
  createTables(db);

  run(db)
    .then((res) => {
      db.close();
      console.log("Success.  workspaces and bases tables populdated");
    })
    .catch((err) => console.log("ERR: ", err));


}

module.exports = {
  run: run,
  createTables: createTables
}