# Bulk Export Records from Airtable Enterprise Account

As an Airtable admin it is sometimes desireable to export all of the data that lives in your Enterprise environment. These cirucmstances include:

1. You want to monitor what data users are sotring in Airtable
2. You want to create an internally managed backup separate from Airtable
3. You want to move this data into your Enterprise data warehouse to make it available to other downstream applications

This repository contains a script that can be accomplish these goals.

At a high level this script will:

1. Use the Admin API to build a list of all workspaces and bases in your environment and write them to a SQLite database
2. For each base:

   1. Use the [Admin API](https://airtable.com/api/enterprise) to add an admin as a read only collaborator to the base
   2. Use the [Metadata API](https://airtable.com/api/v2) to get a list of all tables within the base
   3. Use the standard [Airtable API](https://airtable.com/api) to read all records out of those tables

3. Write those records to the same SQLite database

From there you can perform additional analytics on this data to accomplish your end goal.

## Running the Script

Install all packages using:
`npm install`

Then run `npm run start [-- --scan_id=some_scan_id --delete_data]`

## SQLite Database

The scripts write information to a SQLite database across three tables:

1. `workspaces`: all of the workspaces in the Enterprise account
2. `bases`: all bases in those workspaces
3. `data`: the records in each of those bases

## Building Metadata vs Downloading Data and using Scan ID

There are two steps to the script. The first (contained in `metadata.js`) is building a layout of the current state of the Enterprise account. This involves looking up all workspaces in your enterprise environment, and then examining all bases in those workspaces. This is accomplished by using Airtable's Admin API.

The second (contained in `base_data.js`) is looking at the output from generating this metadata and then pulling the data from each base.

Each of these files can be run independently. You have to build the metadata first so that you know what bases to scan. Scanning all of those bases can take time and may die mid scan. In these cases, it doesn't necessarily make sense to rebuild the set of workspaces and bases, and it also doesn't make sense to restart the scan from the beginning of all bases.

For this, we've provided a "scan ID" that is tracked on the base and data level. The Scan ID is a UUID that is created at the beginning of the script. Each record in the `data` table will have an associated Scan ID. Once all records have been written for a given base, the Scan ID is then written to the `bases` table.

Using this, you can then restart the script without rescanning bases that have already been recently processed. You can provide the optional `--scan_id` parameter. When you do this, the script will:

1. Ignore any bases that have already been marked with that Scan ID
2. Mark all bases and records it does scan with that Scan ID

For example, if your Scan ID is `11b68d3c-66b0-4cac-a6d4-7458cbc5b1f2`, and you have 1000 bases in your Enterprise Account. The script only makes it through 80% of those bases (for any number of reasons), and you want to restart it to complete the operation, you can run the script as:
`npm run -- --scan_id='11b68d3c-66b0-4cac-a6d4-7458cbc5b1f2'`

It will ignore the 80% of bases already scanned, and only pick up the remaining 20%.

## Handling Data Deletion

Data may leave the Enterprise Account, for example a user deletes a base or deletes a handful of records that are no longer necessary. In these cases, there are a few different ways you can handle this. One is to remove the data from the SQLite database such that it is an accurate reflection of what is currently available in your environment. The other is to leave the data alone so that you can retroactively look at what data was in your environment at any given point in time.

The `--delete_data` flag changes the behavior of the script. By default, this flag is not included and so **no** data is removed at the end of the script. In this case, when the script finishes succesfully, there will be different Scan IDs in your base. Bases and records that do not have the most recent Scan ID no longer exist in your Enterprise account.

If you provide the `--delete_data` flag, at the end of a successfull execution of the script, it will identify all bases and records that do not match the provided Scan ID and delete from the SQLite database.
