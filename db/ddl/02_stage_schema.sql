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
  service_id        text,
  date              date,
  exception_type    smallint,
  PRIMARY KEY (service_id, date)
);

CREATE TABLE stage_gtfs.shapes (
  shape_id            text,
  shape_pt_lat        double precision,
  shape_pt_lon        double precision,
  shape_pt_sequence   integer,
  shape_dist_traveled double precision,
  PRIMARY KEY (shape_id, shape_pt_sequence)
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
  trip_id               text,
  arrival_time          interval,
  departure_time        interval,
  stop_id               integer,
  stop_sequence         smallint,
  stop_headsign         text,
  pickup_type           smallint,
  drop_off_type         smallint,
  shape_dist_traveled   double precision,
  timepoint             boolean,
  PRIMARY KEY (trip_id, stop_sequence)
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

/*
 * # Derived GTFS tables
 */

CREATE TABLE stage_gtfs.stop_sequences_for_routing (
  fid               serial        PRIMARY KEY,
  trip_ids          text[]        NOT NULL,
  stop_ids  integer[]     NOT NULL
);
COMMENT ON TABLE stage_gtfs.stop_sequences_for_routing IS
'Unique, ordered stop sequences of trips
such that GTFS trips sharing same sequences
need not be routed to network one by one.';
CREATE INDEX ON stage_gtfs.stop_sequences_for_routing
  USING GIN (trip_ids);
CREATE INDEX ON stage_gtfs.stop_sequences_for_routing
  USING GIN (stop_ids);

CREATE OR REPLACE FUNCTION stage_gtfs.collect_unique_stop_sequences()
RETURNS TABLE (
  n_total                 bigint,
  min_trip_ids_per_array  bigint,
  max_trip_ids_per_array  bigint,
  min_stop_ids_per_array  bigint,
  max_stop_ids_per_array  bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  IF EXISTS (
    SELECT *
    FROM stage_gtfs.stop_sequences_for_routing
    LIMIT 1
  ) THEN
    RAISE EXCEPTION 'Table stage_gtfs.stop_sequences_for_routing is not empty!'
    USING HINT = 'Truncate the table first.';
  END IF;

  RETURN QUERY
  WITH
    ordered_stop_times AS (
      SELECT trip_id, stop_id, stop_sequence
      FROM stage_gtfs.stop_times
      ORDER BY trip_id, stop_sequence
    ),
    arrays_per_trip AS (
      SELECT trip_id,
        array_agg(stop_id) AS stop_ids
      FROM ordered_stop_times
      GROUP BY trip_id
    ),
    unique_arrays_inserted AS (
      INSERT INTO stage_gtfs.stop_sequences_for_routing (trip_ids, stop_ids)
      SELECT array_agg(trip_id) AS trip_ids,
        stop_ids
      FROM arrays_per_trip
      GROUP BY stop_ids
      RETURNING *
    )
    SELECT
      count(trip_ids)::bigint                 AS n_total,
      min(array_length(trip_ids, 1))::bigint  AS min_trip_ids_per_array,
      max(array_length(trip_ids, 1))::bigint  AS max_trip_ids_per_array,
      min(array_length(stop_ids, 1))::bigint  AS min_stop_ids_per_array,
      max(array_length(stop_ids, 1))::bigint  AS max_stop_ids_per_array
    FROM unique_arrays_inserted;
END;
$$;

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
