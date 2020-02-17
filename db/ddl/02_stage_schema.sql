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
 * CREATE TABLE stage_osm.raw_bus_lines (
 *   fid                          serial    PRIMARY KEY,
 *   osm_id                       varchar,  -- Unique, will cast to int
 *   oneway                       varchar,  -- Only values 'yes', 'no', null
 *   highway                      varchar,  -- Text values
 *   lanes                        varchar,  -- Will cast to int
 *   geom                         geometry(LINESTRING, 4326)
 * );
 * CREATE TABLE stage_osm.raw_tram_lines (
 *   fid                          serial    PRIMARY KEY,
 *   osm_id                       varchar,  -- Unique, will cast to int
 *   tram_segregation_physical    varchar,  -- Text values
 *   geom                         geometry(LINESTRING, 4326)
 * );
 */

CREATE TABLE stage_osm.combined_lines (
  osm_id                      bigint              PRIMARY KEY,
  oneway                      boolean             NOT NULL,
  mode                        public.mode_type    NOT NULL,
  highway                     text,
  lanes                       smallint,
  tram_segregation_physical   text,
  geom                        geometry(LINESTRING, 3067)
);

CREATE INDEX combined_lines_geom_idx
  ON stage_osm.combined_lines
  USING GIST (geom);

CREATE OR REPLACE FUNCTION stage_osm.populate_combined_lines()
RETURNS TABLE (
  mode            public.mode_type,
  rows_inserted   bigint
)
LANGUAGE SQL
VOLATILE
AS $$
WITH
  bus_cast AS (
    SELECT
      osm_id::bigint                AS osm_id,
      CASE
        WHEN oneway = 'yes' THEN true
        WHEN oneway = 'no' THEN false
        WHEN oneway IS NULL THEN false
      END                           AS oneway,
      'bus'::public.mode_type       AS mode,
      highway::text                 AS highway,
      round(lanes::real)::smallint  AS lanes,  -- There may appear values like '1.8' ...
      NULL::text                    AS tram_segregation_physical,
      ST_Transform(geom, 3067)      AS geom
    FROM stage_osm.raw_bus_lines
  ),
  tram_cast AS (
    SELECT
      osm_id::bigint                    AS osm_id,
      true                              AS oneway,
      'tram'::public.mode_type          AS mode,
      NULL::text                        AS highway,
      NULL::smallint                    AS lanes,
      tram_segregation_physical::text   AS tram_segregation_physical,
      ST_Transform(geom, 3067)          AS geom
    FROM stage_osm.raw_tram_lines
  ),
  combined_insert AS (
    INSERT INTO stage_osm.combined_lines
    SELECT * FROM bus_cast
    UNION
    SELECT * FROM tram_cast
    ON CONFLICT DO NOTHING
    RETURNING mode
  )
  SELECT
    mode,
    count(mode) AS rows_inserted
  FROM combined_insert
  GROUP BY mode;
  -- TODO: SELECT these into a logging table
$$;

COMMIT;

BEGIN;
\echo Creating stage_gtfs schema ...

CREATE SCHEMA IF NOT EXISTS stage_gtfs;

CREATE TABLE stage_gtfs.routes (
  route_id          text        PRIMARY KEY,
  agency_id         text,
  route_short_name  text,
  route_long_name   text,
  route_desc        text,
  route_type        smallint,
  route_url         text
);

CREATE TABLE stage_gtfs.calendar (
  service_id        text        PRIMARY KEY,
  monday            boolean,
  tuesday           boolean,
  wednesday         boolean,
  thursday          boolean,
  friday            boolean,
  saturday          boolean,
  sunday            boolean,
  start_date        date,
  end_date          date
);

CREATE TABLE stage_gtfs.calendar_dates (
  service_id        text        PRIMARY KEY,
  date              date,
  exception_type    smallint
);

CREATE TABLE stage_gtfs.shapes (
  shape_id            text        PRIMARY KEY,
  shape_pt_lat        double precision,
  shape_pt_lon        double precision,
  shape_pt_sequence   integer,
  shape_dist_traveled double precision
);

CREATE TABLE stage_gtfs.trips (
  route_id              text,
  service_id            text,
  trip_id               text        PRIMARY KEY,
  trip_headsign         text,
  direction_id          smallint,
  shape_id              text,
  wheelchair_accessible smallint,
  bikes_allowed         smallint,
  max_delay             smallint
);

CREATE TABLE stage_gtfs.stop_times (
  trip_id               text        PRIMARY KEY,
  arrival_time          interval,
  departure_time        interval,
  stop_id               integer,
  stop_sequence         smallint,
  stop_headsign         text,
  pickup_type           smallint,
  drop_off_type         smallint,
  shape_dist_traveled   double precision,
  timepoint             boolean
);

CREATE TABLE stage_gtfs.stops (
  stop_id               integer     PRIMARY KEY,
  stop_code             text,
  stop_name             text,
  stop_desc             text,
  stop_lat              double precision,
  stop_lon              double precision,
  zone_id               text,
  stop_url              text,
  location_type         smallint,
  parent_station        integer,
  wheelchair_boarding   smallint,
  platform_code         text,
  vehicle_type          smallint
);

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
