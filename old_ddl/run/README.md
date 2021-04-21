# `db/run`

Populate the database with these scripts.
After these, you should be ready to import HFP data dumps into the database.

## Prerequisites

First you must successfully build the database by running the DDL scripts in [`db/ddl`](../ddl).

If you import the OSM data from `.osm` files, you have to use the shell scripts in [`scripts`](../../scripts) that will populate `stage_osm.raw_bus_lines` and `stage_osm.raw_tram_lines` using `ogr2ogr`.
Alternatively, you can use `002_import_osm_dumps.sql` here, if you have the database-compatible csv files on the server (created by the author).

Finally, you should be connected to the empty database and run these scripts in the correct order interactively, e.g. using psql's `\i <script_file>.sql`.
Fully automated execution is not recommended since the data may need some intermediate manual checks.
