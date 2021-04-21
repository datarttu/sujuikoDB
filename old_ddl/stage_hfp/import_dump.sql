DROP FUNCTION IF EXISTS stage_hfp.route_exists_in_patterns;
CREATE FUNCTION stage_hfp.route_exists_in_patterns(
  route             text
)
RETURNS BOOLEAN
LANGUAGE PLPGSQL
AS $$
DECLARE
  res     boolean;
BEGIN
  EXECUTE format(
    $s$
    SELECT EXISTS (SELECT 1 FROM sched.patterns WHERE route = %L)
    $s$,
    route
  ) INTO res;
  RETURN res;
END;
$$;

DROP PROCEDURE IF EXISTS stage_hfp.import_dump;
CREATE PROCEDURE stage_hfp.import_dump(
  route             text,
  oday              date,
  gz_path_template  text,
  sid               text        DEFAULT NULL,
  -- Query is canceled if one statement takes more than 15 minutes
  -- = 900000 ms to complete.
  timeout           integer     DEFAULT 900000
  -- TODO: Add filter & invalidation parameters?
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  gz_path       text;
  program_call  text;
  msg           text;
BEGIN

  gz_path := format(gz_path_template, oday, route);
  -- TODO: Prevent possible injection risk
  program_call := format('gzip -cd %s', gz_path);

  IF sid IS NULL THEN
    sid := format('%s_%s %s', route, oday, now());
  END IF;

  CALL stage_hfp.log_step(
    session_id  := sid,
    step        := 'Import started',
    route       := route,
    oday        := oday,
    raw_file    := gz_path
  );

  -- If we do not have corresponding schedule patterns and segments
  -- for the selected route, we do not want to start the import process at all.
  IF NOT stage_hfp.route_exists_in_patterns(route := route) THEN
    RAISE EXCEPTION 'No patterns for route %', route;
  END IF;

  -- Import is done using temporary tables lasting only during the transaction.
  CREATE TEMPORARY TABLE raw (
    LIKE stage_hfp.raw INCLUDING ALL
  ) ON COMMIT DROP;
  CREATE TRIGGER t10_ignore_invalid_raw_rows
  BEFORE INSERT ON raw
  FOR EACH ROW
  EXECUTE PROCEDURE stage_hfp.ignore_invalid_raw_rows();

  CREATE TRIGGER t20_set_raw_additional_fields
  BEFORE INSERT ON raw
  FOR EACH ROW
  EXECUTE PROCEDURE stage_hfp.set_raw_additional_fields();

  CREATE TEMPORARY TABLE journeys (
    LIKE stage_hfp.journeys INCLUDING ALL
  ) ON COMMIT DROP;

  CREATE TEMPORARY TABLE jrn_points (
    LIKE stage_hfp.jrn_points INCLUDING ALL
  ) ON COMMIT DROP;

  CREATE TEMPORARY TABLE jrn_segs (
    LIKE stage_hfp.jrn_segs INCLUDING ALL
  ) ON COMMIT DROP;

  EXECUTE format(
    $s$
    COPY raw (
      is_ongoing, event_type, dir, oper, veh, tst,
      lat, lon, odo, drst, oday, start, loc, stop, route
    )
    FROM PROGRAM %1$L
    WITH CSV
    $s$,
    program_call
  );

  CALL stage_hfp.log_step(session_id := sid, step := 'Raw copied');

  PERFORM stage_hfp.set_obs_nums('raw');
  PERFORM stage_hfp.set_raw_movement_values('raw');
  PERFORM stage_hfp.delete_duplicates_by_tst('raw');
  PERFORM stage_hfp.drop_with_invalid_movement_values(
    target_table  := 'raw',
    max_spd       := 28.0,
    min_acc       := -5.0,
    max_acc       := 5.0
  );

  CALL stage_hfp.log_step(session_id := sid, step := 'Raw fixed');

  PERFORM stage_hfp.extract_journeys_from_raw(
    raw_table       := 'raw',
    journey_table   := 'journeys'
  );
  PERFORM stage_hfp.set_journeys_ttid_ptid('journeys');
  PERFORM invalidate(
    tb_name   := 'journeys',
    reason    := 'NULL ttid',
    where_cnd := 'ttid IS NULL'
  );
  PERFORM invalidate(
    tb_name   := 'journeys',
    reason    := 'Too few observations vs. tst span',
    where_cnd := 'coalesce(n_obs::numeric / nullif(extract(epoch FROM rn_length(tst_span))::numeric, 0.0), 0.0) < 0.5'
  );
  PERFORM invalidate(
    tb_name   := 'journeys',
    reason    := 'Too little odometer sum vs. cumulative GPS distance',
    where_cnd := 'coalesce(rn_length(odo_span)::numeric / nullif(raw_distance, 0.0), 0.0) < 0.5'
  );
  PERFORM invalidate(
    tb_name   := 'journeys',
    reason    := 'Non-increasing odometer values',
    where_cnd := 'n_neg_dodo > 0'
  );
  PERFORM invalidate(
    tb_name   := 'journeys',
    reason    := 'Defect door status: open in > 50 % of obs',
    where_cnd := 'n_dropen::real / n_obs::real > 0.5'
  );
  PERFORM stage_hfp.discard_invalid_journeys(
    target_table      := 'journeys',
    log_to_discarded  := true
  );

  CALL stage_hfp.log_step(session_id := sid, step := 'Journeys made');

  IF NOT EXISTS (SELECT 1 FROM journeys) THEN
    RAISE EXCEPTION 'No valid journeys left';
  END IF;

  PERFORM stage_hfp.extract_jrn_points_from_raw(
    raw_table       := 'raw',
    jrn_point_table := 'jrn_points',
    journey_table   := 'journeys'
  );
  PERFORM stage_hfp.mark_redundant_jrn_points('jrn_points');
  PERFORM stage_hfp.discard_redundant_jrn_points('jrn_points');
  PERFORM stage_hfp.cluster_halted_points(
    jrn_point_table := 'jrn_points',
    min_clust_size  := 3
  );
  PERFORM stage_hfp.set_segment_candidates(
    jrn_point_table := 'jrn_points',
    max_distance    := 20.0
  );
  PERFORM stage_hfp.discard_outlier_points('jrn_points');
  PERFORM stage_hfp.set_best_match_segments('jrn_points');
  PERFORM stage_hfp.discard_failed_seg_matches('jrn_points');
  PERFORM stage_hfp.set_linear_locations('jrn_points');
  PERFORM stage_hfp.set_linear_movement_values('jrn_points');

  CALL stage_hfp.log_step(session_id := sid, step := 'Points made');

  PERFORM stage_hfp.extract_jrn_segs_from_jrn_points(
    jrn_point_table   := 'jrn_points',
    journey_table     := 'journeys',
    jrn_segs_table    := 'jrn_segs'
  );
  PERFORM stage_hfp.set_seg_firstlast_values(
    jrn_segs_table    := 'jrn_segs',
    jrn_point_table   := 'jrn_points'
  );
  PERFORM stage_hfp.interpolate_enter_exit_ts('jrn_segs');
  PERFORM stage_hfp.set_pt_timediffs_array('jrn_segs');
  PERFORM stage_hfp.set_n_halts('jrn_segs');
  PERFORM stage_hfp.set_journeys_seg_aggregates(
    jrn_segs_table  := 'jrn_segs',
    journey_table   := 'journeys'
  );
  PERFORM invalidate(
    tb_name   := 'journeys',
    reason    := 'No valid segment matches',
    where_cnd := 'n_segs = 0'
  );
  PERFORM stage_hfp.discard_invalid_journeys(
    target_table      := 'journeys',
    log_to_discarded  := true
  );

  CALL stage_hfp.log_step(session_id := sid, step := 'Segments made');

  PERFORM stage_hfp.transfer_journeys('journeys');

  CALL stage_hfp.log_step(session_id := sid, step := 'Journeys transferred');

  PERFORM stage_hfp.transfer_segs('jrn_segs');

  CALL stage_hfp.log_step(session_id := sid, step := 'Segments transferred, all done');

EXCEPTION
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS msg := MESSAGE_TEXT;
    msg := format('Abort: %s', msg);
    CALL stage_hfp.log_step(
      session_id := sid,
      route := route,
      oday := oday,
      step := msg);
END;
$$;
COMMENT ON PROCEDURE stage_hfp.import_dump IS
'Imports a raw data dump through transformation and validation to `obs` schema.
A `.csv.gz` data dump file shall contain data for one route and operating day.
- `route`:            route to import, as it appears in `sched` schema
- `oday`:             operating day as `yyyy-mm-dd`
- `gz_path_template`: full path of data dump file where the `oday` part has
                      the placeholder "%1$s" and `route` part "%2$s"
- `sid`               unique session id for logging; generated automatically if empty
- `timeout`:          abort timeout for one statement (default 15 mins = 900000 ms)';
