/*
 * Import raw OSM data dumps from csv files.
 * These are not available in this format originally
 * but created by Arttu K 6/2020.
 * To import original data, see the `scripts/`
 * directory to download the data and import it
 * according to the specified configuration.
 */
\set ON_ERROR_STOP on

BEGIN;

COPY stage_osm.raw_bus_lines FROM '/data1/osm/raw_bus_lines.csv' WITH CSV HEADER;
COPY stage_osm.raw_tram_lines FROM '/data1/osm/raw_tram_lines.csv' WITH CSV HEADER;

COMMIT;
