/*
 * Import GTFS tables to staging schema.
 * Temporary tables are used to import all data first
 * and then drop everything but data related to HSL tram and bus trips.
 * Note 1:  In HSL GTFS stops.txt, there is a buggy whitespace
 *          in parent_station field that has to be removed first, e.g.:
 *          `sed -i 's/, ,/,,/g' stops.txt`.
 * Note 2:  This script uses server-side COPY FROM.
 *          GTFS files must be in `gtfs_dir` and readable by `postgres`.
 */
\set gtfs_dir           '/data1/gtfs/'
\set routes             :gtfs_dir'routes.txt'
\set trips              :gtfs_dir'trips.txt'
\set calendar           :gtfs_dir'calendar.txt'
\set calendar_dates     :gtfs_dir'calendar_dates.txt'
\set shapes             :gtfs_dir'shapes.txt'
\set stop_times         :gtfs_dir'stop_times.txt'
\set stops              :gtfs_dir'stops.txt'
\set ON_ERROR_STOP on

\c sujuiko;

BEGIN;

\timing on

\qecho Importing routes
CREATE TEMPORARY TABLE tmp_routes (
  LIKE stage_gtfs.routes INCLUDING INDEXES
) ON COMMIT DROP;
COPY tmp_routes FROM :'routes' WITH CSV HEADER;
TRUNCATE TABLE stage_gtfs.routes;
INSERT INTO stage_gtfs.routes (
  SELECT * FROM tmp_routes
  WHERE route_type IN (0, 700, 701, 702, 704)
);

\qecho Importing trips
CREATE TEMPORARY TABLE tmp_trips (
  LIKE stage_gtfs.trips INCLUDING INDEXES
) ON COMMIT DROP;
COPY tmp_trips FROM :'trips' WITH CSV HEADER;
TRUNCATE TABLE stage_gtfs.trips;
INSERT INTO stage_gtfs.trips (
  SELECT * FROM tmp_trips
  WHERE route_id IN (
    SELECT DISTINCT route_id
    FROM stage_gtfs.routes
  )
);

\qecho Importing calendar
CREATE TEMPORARY TABLE tmp_calendar (
  LIKE stage_gtfs.calendar INCLUDING INDEXES
) ON COMMIT DROP;
COPY tmp_calendar FROM :'calendar' WITH CSV HEADER;
TRUNCATE TABLE stage_gtfs.calendar;
INSERT INTO stage_gtfs.calendar (
  SELECT * FROM tmp_calendar
  WHERE service_id IN (
    SELECT DISTINCT service_id
    FROM stage_gtfs.trips
  )
);

\qecho Importing calendar_dates
CREATE TEMPORARY TABLE tmp_calendar_dates (
  LIKE stage_gtfs.calendar_dates INCLUDING INDEXES
) ON COMMIT DROP;
COPY tmp_calendar_dates FROM :'calendar_dates' WITH CSV HEADER;
TRUNCATE TABLE stage_gtfs.calendar_dates;
INSERT INTO stage_gtfs.calendar_dates (
  SELECT * FROM tmp_calendar_dates
  WHERE service_id IN (
    SELECT DISTINCT service_id
    FROM stage_gtfs.trips
  )
);

\qecho Importing shapes
CREATE TEMPORARY TABLE tmp_shapes (
  LIKE stage_gtfs.shapes INCLUDING INDEXES
) ON COMMIT DROP;
COPY tmp_shapes FROM :'shapes' WITH CSV HEADER;
TRUNCATE TABLE stage_gtfs.shapes;
INSERT INTO stage_gtfs.shapes (
  SELECT * FROM tmp_shapes
  WHERE shape_id IN (
    SELECT DISTINCT shape_id
    FROM stage_gtfs.trips
  )
);

\qecho Importing stop_times
CREATE TEMPORARY TABLE tmp_stop_times (
  LIKE stage_gtfs.stop_times INCLUDING INDEXES
) ON COMMIT DROP;
COPY tmp_stop_times FROM :'stop_times' WITH CSV HEADER;
TRUNCATE TABLE stage_gtfs.stop_times;
INSERT INTO stage_gtfs.stop_times (
  SELECT * FROM tmp_stop_times
  WHERE trip_id IN (
    SELECT DISTINCT trip_id
    FROM stage_gtfs.trips
  )
);

\qecho Importing stops
CREATE TEMPORARY TABLE tmp_stops (
  LIKE stage_gtfs.stops INCLUDING INDEXES
) ON COMMIT DROP;
COPY tmp_stops FROM :'stops' WITH CSV HEADER;
TRUNCATE TABLE stage_gtfs.stops;
INSERT INTO stage_gtfs.stops (
  SELECT * FROM tmp_stops
  WHERE stop_id IN (
    SELECT DISTINCT stop_id
    FROM stage_gtfs.stop_times
  )
);

\timing off

COMMIT;
