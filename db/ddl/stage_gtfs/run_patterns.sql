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
--SELECT * FROM stage_gtfs.count_pattern_stops_invalidations();
SELECT * FROM invalidate('stage_gtfs.patterns', 'Network path too long', 'nw_vs_shape_coeff > 1.1');
SELECT * FROM invalidate('stage_gtfs.patterns', 'Network path too short', 'nw_vs_shape_coeff < 0.9');
