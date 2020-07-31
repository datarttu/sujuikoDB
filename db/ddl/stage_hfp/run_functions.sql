/*
 * This file includes example function calls of stage_hfp schema
 * as they are intended in a data import process.
 * The CSV file is just an arbitrary example.
 * Do NOT run this as a whole unless you know what you are doing.
 * Run DDL scripts with run_ddl.sql first if needed.
 * COMMIT the result afterwards manually (only if you want!).
 */

\set ON_ERROR_ROLLBACK interactive

BEGIN;

\set route '1017'
\set oday '2019-11-12'
\set raw_file '/data0/hfpdumps/november/hfp_':oday'_routes/route_':route'.csv.gz'
\set sid :oday'_':route'_test'
\set prg_call 'gzip -cd ':raw_file

SELECT stage_hfp.log_step(
  session_id  := :'sid',
  step        := 'Import started',
  route       := :'route',
  oday        := :'oday',
  raw_file    := :'raw_file'
);

-- Copy the raw data. Triggers do some row-level processing already now.
COPY stage_hfp.raw (
  is_ongoing,
  event_type,
  dir,
  oper,
  veh,
  tst,
  lat,
  lon,
  odo,
  drst,
  oday,
  start,
  loc,
  stop,
  route
)
FROM PROGRAM :'prg_call'
WITH CSV;

SELECT stage_hfp.log_step(session_id := :'sid', step := 'Raw copied');
SAVEPOINT copied;

SELECT stage_hfp.set_obs_nums('stage_hfp.raw');
-- Drop unnecessary and invalid values from raw data.
SELECT stage_hfp.set_raw_movement_values('stage_hfp.raw');
SELECT stage_hfp.delete_duplicates_by_tst('stage_hfp.raw');
SELECT * FROM stage_hfp.drop_with_invalid_movement_values(
  target_table  := 'stage_hfp.raw',
  max_spd       := 28.0,
  min_acc       := -5.0,
  max_acc       := 5.0
);

SELECT stage_hfp.log_step(session_id := :'sid', step := 'Raw fixed');
SAVEPOINT raw_done;

-- Transfer data to journeys and jrn_points and start working on them.
SELECT stage_hfp.extract_journeys_from_raw(
  raw_table       := 'stage_hfp.raw',
  journey_table   := 'stage_hfp.journeys'
);
SELECT stage_hfp.set_journeys_ttid_ptid('stage_hfp.journeys');
SELECT * FROM invalidate(
  tb_name   := 'stage_hfp.journeys',
  reason    := 'NULL ttid',
  where_cnd := 'ttid IS NULL'
)
UNION
SELECT * FROM invalidate(
  tb_name   := 'stage_hfp.journeys',
  reason    := 'Too few observations vs. tst span',
  where_cnd := 'coalesce(n_obs::numeric / nullif(extract(epoch FROM rn_length(tst_span))::numeric, 0.0), 0.0) < 0.5'
)
UNION
SELECT * FROM invalidate(
  tb_name   := 'stage_hfp.journeys',
  reason    := 'Too little odometer sum vs. cumulative GPS distance',
  where_cnd := 'coalesce(rn_length(odo_span)::numeric / nullif(raw_distance, 0.0), 0.0) < 0.5'
)
UNION
SELECT * FROM invalidate(
  tb_name   := 'stage_hfp.journeys',
  reason    := 'Non-increasing odometer values',
  where_cnd := 'n_neg_dodo > 0'
)
UNION
SELECT * FROM invalidate(
  tb_name   := 'stage_hfp.journeys',
  reason    := 'Defect door status: open in > 50 % of obs',
  where_cnd := 'n_dropen::real / n_obs::real > 0.5'
);
SELECT stage_hfp.discard_invalid_journeys(
  target_table      := 'stage_hfp.journeys',
  log_to_discarded  := true
);

SELECT stage_hfp.log_step(session_id := :'sid', step := 'Journeys made');
SAVEPOINT journeys_ready;

SELECT stage_hfp.extract_jrn_points_from_raw(
  raw_table       := 'stage_hfp.raw',
  jrn_point_table := 'stage_hfp.jrn_points',
  journey_table   := 'stage_hfp.journeys'
);
SELECT stage_hfp.mark_redundant_jrn_points('stage_hfp.jrn_points');
SELECT stage_hfp.discard_redundant_jrn_points('stage_hfp.jrn_points');
SELECT stage_hfp.cluster_halted_points(
  jrn_point_table := 'stage_hfp.jrn_points',
  min_clust_size  := 3
);
SELECT stage_hfp.set_segment_candidates(
  jrn_point_table := 'stage_hfp.jrn_points',
  max_distance    := 20.0
);
SELECT stage_hfp.discard_outlier_points('stage_hfp.jrn_points');
SELECT stage_hfp.set_best_match_segments('stage_hfp.jrn_points');
SELECT stage_hfp.discard_failed_seg_matches('stage_hfp.jrn_points');
SELECT stage_hfp.set_linear_locations('stage_hfp.jrn_points');
SELECT stage_hfp.set_linear_movement_values('stage_hfp.jrn_points');

SELECT stage_hfp.log_step(session_id := :'sid', step := 'Points made');
SAVEPOINT points_ready;

SELECT stage_hfp.extract_jrn_segs_from_jrn_points(
  jrn_point_table   := 'stage_hfp.jrn_points',
  journey_table     := 'stage_hfp.journeys',
  jrn_segs_table    := 'stage_hfp.jrn_segs'
);
SELECT stage_hfp.set_seg_firstlast_values(
  jrn_segs_table    := 'stage_hfp.jrn_segs',
  jrn_point_table   := 'stage_hfp.jrn_points'
);
SELECT stage_hfp.interpolate_enter_exit_ts('stage_hfp.jrn_segs');
SELECT stage_hfp.set_pt_timediffs_array('stage_hfp.jrn_segs');
SELECT stage_hfp.set_n_halts('stage_hfp.jrn_segs');
SELECT stage_hfp.set_journeys_seg_aggregates(
  jrn_segs_table  := 'stage_hfp.jrn_segs',
  journey_table   := 'stage_hfp.journeys'
);
SELECT * FROM invalidate(
  tb_name   := 'stage_hfp.journeys',
  reason    := 'No valid segment matches',
  where_cnd := 'n_segs = 0'
);
SELECT stage_hfp.discard_invalid_journeys(
  target_table      := 'stage_hfp.journeys',
  log_to_discarded  := true
);

SELECT stage_hfp.log_step(session_id := :'sid', step := 'Segments made');
SAVEPOINT points_ready;

SELECT stage_hfp.transfer_journeys('stage_hfp.journeys');

SELECT stage_hfp.log_step(session_id := :'sid', step := 'Journeys transferred');
SAVEPOINT journeys_transferred;

SELECT step, step_duration, total_duration
FROM stage_hfp.view_log_steps
WHERE session_id = :'sid'
ORDER BY total_duration;
