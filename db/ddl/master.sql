/*
 * Create sujuiko database and execute DDL scripts from scratch.
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on

CREATE DATABASE sujuiko;

\ir extensions.sql

BEGIN;
\ir types.sql
COMMIT;

BEGIN;
\ir stage_gtfs/create_schema.sql
\ir stage_gtfs/base_tables.sql
\ir stage_gtfs/service_dates.sql
\ir stage_gtfs/shape_lines.sql
\ir stage_gtfs/populate_routes_from_gtfs.sql
\ir stage_gtfs/stops_with_mode.sql
\ir stage_gtfs/successive_stops.sql
\ir stage_gtfs/trips_with_dates.sql
\ir stage_gtfs/normalized_stop_times.sql
\ir stage_gtfs/trip_template_arrays.sql
COMMIT;

BEGIN;
\ir stage_hfp/create_schema.sql
\ir stage_hfp/base_tables.sql
COMMIT;

BEGIN;
\i 02_stage_osm_schema.sql
\i 02_stage_hfp_schema.sql
\i 03_network_schema.sql
\i 04_schedule_schema.sql
\i 05_observation_schema.sql
COMMIT;
