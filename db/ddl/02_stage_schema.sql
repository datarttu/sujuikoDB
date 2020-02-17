/*
 * Create tables for the staging schema.
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\c sujuiko;

/*
 * TODO: GTFS, OSM and HFP tables.
 *
 * As for raw data:
 *
 * - GTFS: use pre-defined tables
 * - OSM: we are interested in a few tags only in the end,
 *   and we may want to let osm2pgsql create raw data table on the fly
 *   when importing data.
 * - HFP: use pre-defined tables
 */

BEGIN;
\echo Creating stage_osm schema ...

CREATE SCHEMA IF NOT EXISTS stage_osm;

/*
 * Following tables are created by ogr2ogr when importing data:
 * - stage_osm.raw_bus_lines
 * - stage_osm.raw_tram_lines
 */

COMMIT;

BEGIN;
\echo Creating stage_gtfs schema ...

CREATE SCHEMA IF NOT EXISTS stage_gtfs;

COMMIT;

BEGIN;
\echo Creating stage_hfp schema ...

CREATE SCHEMA IF NOT EXISTS stage_hfp;

CREATE TABLE stage_hfp.raw (
  is_ongoing    boolean,
  event_type    text,
  dir           smallint,
  oper          smallint,
  veh           integer,
  tst           timestamptz,
  lat           real,
  lon           real,
  odo           integer,
  drst          boolean,
  oday          date,
  start         interval,
  loc           text,
  stop          integer,
  route         text
);

COMMIT;
