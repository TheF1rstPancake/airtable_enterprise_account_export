const metadata = require('./metadata');
const base_data = require('./base_data');
const sqlite3 = require('sqlite3');
const uuid = require('uuid');

const argv = require('yargs')
  .scriptName("bulk-export")
  .argv;

if (require.main === module) {
  var db = new sqlite3.Database('export.sqlite');

  // create the scan ID
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

  var tic = new Date();
  metadata.createTables(db);
  metadata.run(db)
    .then(() => {
      console.log("Success.  workspaces and bases tables populdated");

      // close the database to ensure all transactions complete
      db.close();

      // reopen the connection and pass to the base scanning
      db = new sqlite3.Database('export.sqlite');
      console.log("Going to fetch data: ", scan_id);
      return base_data.run(db, scan_id, delete_data);

    })
    .then(() => {
      var toc = new Date();
      console.log(`Success ${scan_id}.  Base data parsed`);
      console.log(`Operation took ${(toc.getTime()-tic.getTime())/1000} seconds`);
    })
    .catch((err) => {
      console.log("Error with script: ", err);
    })
}