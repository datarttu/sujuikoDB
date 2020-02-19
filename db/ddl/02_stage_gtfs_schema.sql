/*
 * Create tables for the GTFS staging schema.
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\c sujuiko;

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

CREATE INDEX stop_times_stop_id_idx
  ON stage_gtfs.stop_times (stop_id);

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

CREATE TABLE stage_gtfs.successive_stops (
  i_stop        integer           NOT NULL,
  j_stop        integer           NOT NULL,
  mode          public.mode_type  NOT NULL,
  PRIMARY KEY (i_stop, j_stop, mode)
);
COMMENT ON TABLE stage_gtfs.successive_stops IS
'Stop pairs that occur in schedules,
unique by transport mode.
For finding network routes between stops.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_successive_stops()
RETURNS TABLE (
  mode            public.mode_type,
  rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  IF EXISTS (
    SELECT *
    FROM stage_gtfs.successive_stops
    LIMIT 1
  ) THEN
    RAISE EXCEPTION 'Table stage_gtfs.successive_stops is not empty!'
    USING HINT = 'Truncate the table first.';
  END IF;

  RETURN QUERY
  WITH
    successive_all AS (
      -- TODO: Make filtered table for bus & tram stop times first!
    ),
    successive_inserted AS (
      INSERT INTO stage_gtfs.successive_stops
      SELECT DISTINCT mode, i_stop, j_stop
      FROM successive_all
      WHERE j_stop IS NOT NULL
      RETURNING *
    )
  SELECT
    mode, count(mode)::bigint AS rows_inserted
  FROM successive_inserted;
END;
$$;

COMMIT;
