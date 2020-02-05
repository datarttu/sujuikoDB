/*
Create tables for the staging schema.

Arttu K 2020-02
*/
\c sujuiko;

/*
TODO: GTFS, OSM and HFP tables.

As for raw data:

- GTFS: use pre-defined tables
- OSM: we are interested in a few tags only in the end,
  and we may want to let osm2pgsql create raw data table on the fly
  when importing data.
- HFP: use pre-defined tables
*/
