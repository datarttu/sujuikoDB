/*
 * GTFS related data transformations.
 * These are needed to prepare the GTFS data for the
 * network and schedule models.
 */
\set ON_ERROR_STOP on

BEGIN;

SELECT * FROM stage_gtfs.fix_direction_id();

-- This will populate sched.routes:
SELECT * FROM stage_gtfs.populate_routes_from_gtfs();

SELECT stage_gtfs.populate_shape_lines();
SELECT stage_gtfs.populate_service_dates();
SELECT stage_gtfs.populate_trips_with_dates();
SELECT stage_gtfs.populate_normalized_stop_times();
SELECT * FROM stage_gtfs.populate_stops_with_mode();
SELECT * FROM stage_gtfs.extract_trip_stop_patterns();
SELECT * FROM stage_gtfs.set_pattern_stops_shape_geoms();

COMMIT;
