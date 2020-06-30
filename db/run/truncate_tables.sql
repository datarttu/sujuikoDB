/*
 * TRUNCATE commands in correct order:
 * execute these to revert to a certain state in the data import process.
 * WARNING: Executing the entire script will empty all the tables!
 *          Normally you will want to pick just the necessary commands.
 *          Consider BEGINning a transaction first and only then executing these.
 */

-- obs schema: TO DO.

-- sched schema, template side; obs.journeys needs to be empty first.
TRUNCATE TABLE sched.segment_times;
TRUNCATE TABLE sched.template_timestamps;
TRUNCATE TABLE sched.templates CASCADE;

-- sched schema, pattern side.
TRUNCATE TABLE sched.segments;
TRUNCATE TABLE sched.patterns CASCADE;

-- stage_gtfs schema, templates and patterns
TRUNCATE TABLE stage_gtfs.template_timestamps;
TRUNCATE TABLE stage_gtfs.template_stops;
TRUNCATE TABLE stage_gtfs.templates CASCADE;
TRUNCATE TABLE stage_gtfs.pattern_stops CASCADE;
TRUNCATE TABLE stage_gtfs.pattern_paths;
TRUNCATE TABLE stage_gtfs.stop_pairs;
TRUNCATE TABLE stage_gtfs.stop_pair_paths;

-- nw schema
TRUNCATE TABLE nw.stops;
TRUNCATE TABLE nw.nodes CASCADE;
TRUNCATE TABLE nw.links_vertices_pgr;
TRUNCATE TABLE nw.links CASCADE;

-- stage_nw schema
TRUNCATE TABLE stage_nw.snapped_stops;
TRUNCATE TABLE stage_nw.contracted_nw;
TRUNCATE TABLE stage_nw.contracted_edges_to_merge;
TRUNCATE TABLE stage_nw.contracted_arr;
TRUNCATE TABLE stage_nw.raw_nw_vertices_pgr;
TRUNCATE TABLE stage_nw.raw_nw;

-- sched schema, routes
TRUNCATE TABLE sched.routes CASCADE;

-- stage_osm schema, combined_lines
TRUNCATE TABLE stage_osm.combined_lines;

-- stage_gtfs schema, GTFS staging stuff
TRUNCATE TABLE stage_gtfs.normalized_stop_times;
TRUNCATE TABLE stage_gtfs.trips_with_dates;
TRUNCATE TABLE stage_gtfs.shape_lines;
TRUNCATE TABLE stage_gtfs.stops_with_mode;

-- Original gtfs tables
TRUNCATE TABLE stage_gtfs.calendar;
TRUNCATE TABLE stage_gtfs.calendar_dates;
TRUNCATE TABLE stage_gtfs.stop_times;
TRUNCATE TABLE stage_gtfs.shapes;
TRUNCATE TABLE stage_gtfs.trips;
TRUNCATE TABLE stage_gtfs.stops;
TRUNCATE TABLE stage_gtfs.routes;

-- Original OSM data
TRUNCATE TABLE stage_gtfs.raw_tram_lines;
TRUNCATE TABLE stage_gtfs.raw_bus_lines;
