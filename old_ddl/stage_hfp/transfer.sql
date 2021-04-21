DROP FUNCTION IF EXISTS stage_hfp.transfer_journeys;
CREATE FUNCTION stage_hfp.transfer_journeys(
  journey_table     regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_jrn   bigint;
  cnt_ins   bigint;
BEGIN
  EXECUTE format(
    $s$
    SELECT count(*) FROM %1$s
    $s$,
    journey_table
  ) INTO cnt_jrn;

  EXECUTE format(
    $s$
    WITH
      inserted AS (
        INSERT INTO obs.journeys (
          jrnid, start_ts, ttid, oper, veh,
          n_obs, n_dropen, tst_span, odo_span, raw_distance
        )
        SELECT
          jrnid,
          start_ts,
          ttid,
          oper,
          veh,
          n_obs,
          n_dropen,
          tst_span,
          odo_span,
          raw_distance
        FROM %1$s
        ON CONFLICT DO NOTHING
        RETURNING 1
      )
    SELECT count(*) FROM inserted;
    $s$,
    journey_table
  ) INTO cnt_ins;

  IF cnt_ins <> cnt_jrn THEN
    RAISE WARNING '% of % journeys transferred', cnt_ins, cnt_jrn;
  END IF;

  RETURN cnt_ins;
END;
$$;
COMMENT ON FUNCTION stage_hfp.transfer_journeys IS
'Transfer final journeys from `journey_table` into `obs.journeys` table.
Rows breaking any constraint (pkey, fkey, not null) in `obs.journeys`
are ignored, and a warning is issued if the number of successfully inserted rows
differs from the number of rows in `journey_table`.';

DROP FUNCTION IF EXISTS stage_hfp.transfer_segs;
CREATE FUNCTION stage_hfp.transfer_segs(
  jrn_segs_table    regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_seg   bigint;
  cnt_ins   bigint;
BEGIN
  EXECUTE format(
    $s$
    SELECT count(*) FROM %1$s
    $s$,
    jrn_segs_table
  ) INTO cnt_seg;

  EXECUTE format(
    $s$
    WITH
      inserted AS (
        INSERT INTO obs.segs (
          jrnid, enter_ts, exit_ts, segno, linkid, reversed,
          end_segment, n, n_halts, thru_s, halted_s, door_s,
          pt_timediffs_s, pt_seg_locs_m, pt_speeds_m_s, pt_doors,
          pt_obs_nums, pt_raw_offsets_m, pt_halt_offsets_m
        )
        SELECT
          jrnid,
          enter_ts,
          exit_ts,
          segno,
          linkid,
          reversed,
          CASE
            WHEN NOT (first_used_seg OR last_used_seg)  THEN 0
            WHEN first_used_seg AND NOT last_used_seg   THEN 1
            WHEN (NOT first_used_seg) AND last_used_seg THEN 2
            WHEN first_used_seg AND last_used_seg       THEN 3
            ELSE NULL
          END                       AS end_segment,
          coalesce(n_valid_obs, 0)  AS n,
          n_halts,
          thru_s,
          halted_s,
          door_s,
          coalesce(pt_timediffs_s, ARRAY[]::real[]),
          coalesce(pt_seg_locs_m, ARRAY[]::real[]),
          coalesce(pt_speeds_m_s, ARRAY[]::real[]),
          coalesce(pt_doors, ARRAY[]::boolean[]),
          coalesce(pt_obs_nums, ARRAY[]::integer[]),
          coalesce(pt_raw_offsets_m, ARRAY[]::real[]),
          coalesce(pt_halt_offsets_m, ARRAY[]::real[])
        FROM %1$s
        WHERE enter_ts IS NOT NULL
          AND exit_ts IS NOT NULL
          AND linkid IS NOT NULL
          AND reversed IS NOT NULL
          AND first_used_seg IS NOT NULL
          AND last_used_seg IS NOT NULL
        ORDER BY jrnid, enter_ts
        ON CONFLICT DO NOTHING
        RETURNING 1
      )
    SELECT count(*) FROM inserted;
    $s$,
    jrn_segs_table
  ) INTO cnt_ins;

  IF cnt_ins <> cnt_seg THEN
    RAISE WARNING '% of % segments transferred', cnt_ins, cnt_seg;
  END IF;

  RETURN cnt_ins;
END;
$$;
COMMENT ON FUNCTION stage_hfp.transfer_segs IS
'Transfer final segments from `jrn_segs_table` into `obs.segs` table.
Encode bool `first_/last_used_seg` fields into `end_segment`
where `f,f=0  t,f=1  f,t=2  t,t=3`.
Rows breaking any unique constraint in `obs.segs` are ignored,
and rows with illegal null values are omitted by WHERE clause.
and a warning is issued if the number of successfully inserted rows
differs from the number of rows in `jrn_segs_table`.';
