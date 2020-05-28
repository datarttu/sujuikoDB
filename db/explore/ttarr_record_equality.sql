SELECT
  a.ttid AS ttid_a,
  b.ttid AS ttid_b,
  a.shape_id = b.shape_id AS shape_id_eq,
  a.stop_ids = b.stop_ids AS stop_ids_eq,
  a.stop_sequences = b.stop_sequences AS stop_sequences_eq,
  a.rel_distances = b.rel_distances AS rel_distances_eq,
  a.arr_time_diffs = b.arr_time_diffs AS arr_time_diffs_eq,
  a.dep_time_diffs = b.dep_time_diffs AS dep_time_diffs_eq,
  a.timepoints = b.timepoints AS timepoints_eq
FROM stage_gtfs.trip_template_arrays        AS a
INNER JOIN stage_gtfs.trip_template_arrays  AS b
  ON  a.ttid <> b.ttid
  AND a.route_id = b.route_id
  AND a.direction_id = b.direction_id
  AND a.dates = b.dates
WHERE a.route_id = '1554'
  AND a.direction_id = 0;
