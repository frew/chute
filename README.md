# Chute

**This project is currently on indefinite hold due some missteps estimating its usefulness. Postmortem [here](https://docs.google.com/document/d/1d8OVC4nYQzW8YGFfk85z1_BfquAbmUk1AhcomdAv8Xw/edit).**

Chute is designed to be an incremental loader for data from a primary datastore (e.g. MySQL, App Engine Datastore) to data warehouses (e.g. Redshift, BigQuery). Similar to [Sqoop](http://sqoop.apache.org/) but relying on incremental changelog updates rather than repeated full database dumps.

Important classes include:
**Importer** - Reads from a datasource and writes each delta event to a StreamProcessor for processing. **MySqlImporter** includes support for reading both changes from the binary log, and creating synthetic deltas to effect a full dumpusing a JDBC reader.
**StreamProcessor** - an interface that process delta events.
**Exporter** - A StreamProcessor that exports the deltas to a data warehouse. **BigQueryExproter** currently supports outputting inserts, but not updates or deletes.
**Row** and **Schema** - defines a common abstraction for a SQL-style row and schema. 

**chute.yaml** holds the configuration parameters for importers and exporters - the example one in the repo contains examples of all available paramaters.

Currently the code is functional for replicating schemas and inserts from MySQL -> BigQuery, but lacks any deduplication functionality or support for updates/deletes. It also restarts from the beginning of the binary log on each run. Additionally, before I would be comfortable putting it in production, I would want to instrument the process with proper logging and monitoring, as well as add tests. 
If you have a use case that Chute might help with, I'd love to hear from you - send me a message!
