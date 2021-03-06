/*
 * Form pattern and template data for the schedule model,
 * and populate the sched schema.
 */
\set ON_ERROR_STOP on

BEGIN;

SELECT * FROM stage_gtfs.extract_trip_stop_patterns();
SELECT * FROM stage_gtfs.set_pattern_stops_shape_geoms();
SELECT * FROM stage_gtfs.extract_unique_stop_pairs();
SELECT * FROM stage_gtfs.find_stop_pair_paths();
SELECT * FROM stage_gtfs.set_pattern_paths();
SELECT * FROM stage_gtfs.set_pattern_stops_path_found();
SELECT * FROM stage_gtfs.set_patterns_length_values();

UPDATE stage_gtfs.pattern_stops SET invalid_reasons = DEFAULT;
UPDATE stage_gtfs.patterns SET invalid_reasons = DEFAULT;

SELECT * FROM invalidate('stage_gtfs.pattern_stops', 'No network path', 'path_found IS false');
SELECT * FROM invalidate('stage_gtfs.pattern_stops', 'Network path too long', 'nw_vs_shape_coeff > 1.2');
SELECT * FROM invalidate('stage_gtfs.pattern_stops', 'Network path too short', 'nw_vs_shape_coeff < 0.8');
SELECT * FROM propagate_invalidations('stage_gtfs.pattern_stops', 'stage_gtfs.patterns', 'ptid');
SELECT * FROM invalidate('stage_gtfs.patterns', 'Network path too long', 'nw_vs_shape_coeff > 1.1');
SELECT * FROM invalidate('stage_gtfs.patterns', 'Network path too short', 'nw_vs_shape_coeff < 0.9');

SELECT * FROM stage_gtfs.extract_trip_templates();
SELECT * FROM invalidate('stage_gtfs.template_stops', 'Non-positive driving time', 'ij_seconds <= 0.0');
SELECT * FROM propagate_invalidations('stage_gtfs.template_stops', 'stage_gtfs.templates', 'ttid');

SELECT * FROM stage_gtfs.transfer_patterns();
SELECT * FROM stage_gtfs.transfer_pattern_segments();

SELECT * FROM stage_gtfs.transfer_templates();
SELECT * FROM stage_gtfs.transfer_template_timestamps();
SELECT * FROM stage_gtfs.transfer_segment_times();

REFRESH MATERIALIZED VIEW sched.mw_pattern_shapes;
REFRESH MATERIALIZED VIEW sched.mw_pattern_stops;

COMMIT;
