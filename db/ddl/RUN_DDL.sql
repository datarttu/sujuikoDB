/*
 * Connect to an empty database (change the name to your database name)
 * and run DDL process from scratch.
 *
 * Arttu K 2020-06
 */

\set ON_ERROR_STOP on

\c test_sujuiko

-- Global objects.
\ir extensions_types.sql
\ir functions.sql

-- Network schema.
\ir nw/base_tables.sql
\ir nw/create_node_table.sql
\ir nw/views.sql

-- Schedule schema.
\ir sched/base_tables.sql
\ir sched/views.sql

-- Observation schema.
\ir obs/base_tables.sql

-- OSM staging schema.
\ir stage_osm/create_schema.sql
\ir stage_osm/combined_lines.sql

-- Network staging schema.
\ir stage_nw/create_schema.sql
\ir stage_nw/raw_nw.sql
\ir stage_nw/contracted_nw.sql
\ir stage_nw/snapped_stops.sql
\ir stage_nw/populate_nw_links.sql
\ir stage_nw/populate_nw_stops.sql

-- Schedule staging (GTFS) schema.
\ir stage_gtfs/base_tables.sql
\ir stage_gtfs/service_dates.sql
\ir stage_gtfs/shape_lines.sql
\ir stage_gtfs/populate_routes_from_gtfs.sql
\ir stage_gtfs/stops_with_mode.sql
\ir stage_gtfs/trips_with_dates.sql
\ir stage_gtfs/normalized_stop_times.sql
\ir stage_gtfs/patterns.sql
\ir stage_gtfs/templates.sql

-- HFP staging schema.
\ir stage_hfp/create_schema.sql
\ir stage_hfp/raw.sql
\ir stage_hfp/journeys.sql
\ir stage_hfp/journey_points.sql
\ir stage_hfp/seg_aggregates.sql
