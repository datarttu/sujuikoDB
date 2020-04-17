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

CREATE FUNCTION stage_gtfs.populate_routes_from_gtfs()
RETURNS TABLE (
 mode            public.mode_type,
 rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
 RETURN QUERY
 WITH
   inserted AS (
     INSERT INTO sched.routes
     SELECT
       route_id AS route,
       (CASE
        WHEN route_type = 0 THEN 'tram'
        WHEN route_type IN (700, 701, 702, 704) THEN 'bus'
        END
       )::public.mode_type AS mode
     FROM stage_gtfs.routes
     ON CONFLICT DO NOTHING
     RETURNING *
   )
 SELECT i.mode, count(i.route)::bigint AS rows_inserted
 FROM inserted AS i
 GROUP BY i.mode
 ORDER BY i.mode;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.populate_routes_from_gtfs IS
'Insert tram and bus routes into sched schema,
with mode indicated as mode_type instead of an integer.
Note that bus mode is NOT based on standard GTFS integer id
but on HSL-specific ids!';

CREATE TABLE stage_gtfs.stops_with_mode (
  stopid        integer           NOT NULL,
  mode          public.mode_type  NOT NULL,
  code          text,
  name          text,
  descr         text,
  parent        integer,
  geom          geometry(POINT, 3067) NOT NULL,
  PRIMARY KEY (stopid, mode)
);
COMMENT ON TABLE stage_gtfs.stops_with_mode IS
'Stops with travel mode; same stop for multiple modes
is indicated by multiple records.';
CREATE INDEX stops_with_mode_geom_idx
  ON stage_gtfs.stops_with_mode
  USING GIST(geom);

CREATE OR REPLACE FUNCTION stage_gtfs.populate_stops_with_mode()
RETURNS TABLE (
  mode            public.mode_type,
  rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RETURN QUERY
  WITH
    mode_routes AS (
      SELECT
        route_id,
        (CASE
         WHEN route_type = 0 THEN 'tram'
         WHEN route_type IN (700, 701, 702, 704) THEN 'bus'
         END
        )::public.mode_type AS mode
      FROM stage_gtfs.routes
    ),
    mode_trips AS (
      SELECT t.trip_id, r.mode
      FROM stage_gtfs.trips     AS t
        INNER JOIN mode_routes  AS r
        ON t.route_id = r.route_id
    ),
    mode_stoptimes AS (
      SELECT DISTINCT st.stop_id, t.mode
      FROM stage_gtfs.stop_times  AS st
      INNER JOIN mode_trips       AS t
      ON st.trip_id = t.trip_id
    ),
    inserted AS (
      INSERT INTO stage_gtfs.stops_with_mode
      SELECT
        s.stop_id     AS stopid,
        m.mode        AS mode,
        s.stop_code   AS code,
        s.stop_name   AS name,
        s.stop_desc   AS desc,
        s.parent_station	AS parent,
        ST_Transform(
          ST_SetSRID(
            ST_MakePoint(s.stop_lon, s.stop_lat), 4326),
          3067) AS geom
      FROM stage_gtfs.stops     AS s
      INNER JOIN mode_stoptimes AS m
      ON s.stop_id = m.stop_id
      ORDER BY s.stop_id, m.mode
      ON CONFLICT DO NOTHING
      RETURNING *
    )
  SELECT i.mode, count(i.stopid)::bigint AS rows_inserted
  FROM inserted AS i
  GROUP BY i.mode
  ORDER BY i.mode;
END;
$$;

CREATE TABLE stage_gtfs.successive_stops (
  i_stop        integer           NOT NULL,
  j_stop        integer           NOT NULL,
  PRIMARY KEY (i_stop, j_stop)
);
COMMENT ON TABLE stage_gtfs.successive_stops IS
'Stop pairs that occur in schedules,
for finding network routes between stops.
Based on ALL bus and tram stops found in the GTFS data.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_successive_stops()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM stage_gtfs.successive_stops;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.successive_stops', cnt;

  INSERT INTO stage_gtfs.successive_stops
  SELECT DISTINCT
    a.stop_id   AS i_stop,
    c.stop_id   AS j_stop
  FROM stage_gtfs.stop_times            AS a
  INNER JOIN stage_gtfs.stops_with_mode AS b
    ON  a.stop_id       = b.stopid
  INNER JOIN stage_gtfs.stop_times      AS c
    ON  a.trip_id       = c.trip_id
    AND a.stop_sequence = (c.stop_sequence - 1);
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% stop pairs inserted into stage_gtfs.successive_stops', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_gtfs.populate_successive_stops IS
'Find unique successive stop pairs
from tram & bus stop times
and insert them into stage_gtfs.successive_stops table.
stage_gtfs.successive_stops is emptied first.';

CREATE TABLE stage_gtfs.normalized_stop_times (
  trip_id               text,
  trip_start_time       interval,
  arr_time_norm         interval,
  dep_time_norm         interval,
  stop_id               integer,
  stop_sequence         smallint,
  shape_dist_traveled   double precision,
  timepoint             boolean,
  PRIMARY KEY (trip_id, stop_sequence)
);
COMMENT ON TABLE stage_gtfs.normalized_stop_times IS
'Stop times of bus and tram trips, where each trip id is assigned
the initial departure time of the trip, and each stop event
is assigned an arrival and a departure time based on that start time.
This is an intermediate step before the trips are grouped into
records with date & initial departure time arrays
based on route, direction, stop sequence and time difference information.';

COMMIT;
