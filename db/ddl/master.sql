/*
 * Create sujuiko database and execute DDL scripts from scratch.
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\set new_db_name test_sujuiko

CREATE DATABASE :new_db_name;
\c :new_db_name

\ir extensions.sql

BEGIN;
\ir types.sql
COMMIT;

BEGIN;
\ir nw/create_schema.sql
\ir nw/base_tables.sql
\ir nw/create_node_table.sql
COMMIT;

BEGIN;
\ir sched/create_schema.sql
\ir sched/base_tables.sql
\ir sched/views.sql
COMMIT;

BEGIN;
\ir obs/create_schema.sql
\ir obs/base_tables.sql
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
\ir stage_osm/create_schema.sql
\ir stage_osm/combined_lines.sql
COMMIT;

BEGIN;
\ir stage_nw/create_schema.sql
\ir stage_nw/raw_nw.sql
\ir stage_nw/contracted_nw.sql
\ir stage_nw/snapped_stops.sql
\ir stage_nw/populate_nw_links.sql
\ir stage_nw/populate_nw_stops.sql
\ir stage_nw/successive_nodes.sql
\ir stage_nw/node_pair_routes.sql
\ir stage_nw/trip_template_routes.sql
\ir stage_nw/transfer_trip_templates.sql
COMMIT;
