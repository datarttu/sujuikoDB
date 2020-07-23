DROP TABLE IF EXISTS stage_hfp.jrn_segs;
CREATE TABLE stage_hfp.jrn_segs (
  jrnid             uuid              NOT NULL,
  segno             smallint          NOT NULL,
  linkid            integer,
  reversed          boolean,
  ij_dist_span      numrange,
  first_used_seg    boolean,
  last_used_seg     boolean,

  -- Arrays
  pt_timestamps     timestamptz[]     DEFAULT '{}',
  pt_timediffs_s    real[]            DEFAULT '{}',
  pt_seg_locs_m     real[]            DEFAULT '{}',
  pt_speeds_m_s     real[]            DEFAULT '{}',
  pt_doors          boolean[]         DEFAULT '{}',
  pt_obs_nums       integer[]         DEFAULT '{}',
  pt_raw_offsets_m  real[]            DEFAULT '{}',
  pt_halt_offsets_m real[]            DEFAULT '{}',

  -- First and last values (by timestamp) for interpolation
  fl_timestamps     timestamptz[2],
  fl_pt_abs_locs    double precision[2],

  -- Interpolated timestamps
  enter_ts          timestamptz,
  exit_ts           timestamptz,

  -- Aggregates
  thru_s            real,
  halted_s          real,
  door_s            real,
  n_halts           smallint,
  n_valid_obs       smallint,

  PRIMARY KEY (jrnid, segno)
);
COMMENT ON TABLE stage_hfp.jrn_segs IS
'Data from `stage_hfp.jrn_points` or corresponding temp table
collected and aggregated by segment level, with interpolated
enter and exit values at segment ends added (by adjacent segments).
This is the last staging table before inserting to `obs` schema.';

CREATE INDEX ON stage_hfp.jrn_segs USING BTREE(linkid, reversed);

DROP FUNCTION IF EXISTS stage_hfp.extract_jrn_segs_from_jrn_points;
CREATE FUNCTION stage_hfp.extract_jrn_segs_from_jrn_points(
  jrn_point_table   regclass,
  journey_table     regclass,
  jrn_segs_table    regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_ins   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
      aggregates_by_seg AS (
        SELECT
          jrnid,
          seg_segno,
          array_agg(tst               ORDER BY obs_num) AS pt_timestamps,
          array_agg(seg_abs_loc::real ORDER BY obs_num) AS pt_seg_locs_m,
          array_agg(pt_spd::real      ORDER BY obs_num) AS pt_speeds_m_s,
          array_agg(drst              ORDER BY obs_num) AS pt_doors,
          array_agg(obs_num           ORDER BY obs_num) AS pt_obs_nums,
          array_agg(raw_offset::real  ORDER BY obs_num) AS pt_raw_offsets_m,
          array_agg(coalesce(halt_offset::real, 0.0)
                                      ORDER BY obs_num) AS pt_halt_offsets_m,
          sum(duration_s) filter(WHERE pt_spd = 0.0
            )::real                                     AS halted_s,
          sum(duration_s) filter(WHERE drst IS true
            )::real                                     AS door_s,
          sum(1 + n_rdnt_after)                         AS n_valid_obs
        FROM %1$s
        GROUP BY jrnid, seg_segno
      ),
      aggr_with_end_segment_flags AS (
        SELECT
          jrnid, seg_segno,
          pt_timestamps, pt_seg_locs_m, pt_speeds_m_s,
          pt_doors, pt_obs_nums, pt_raw_offsets_m, pt_halt_offsets_m,
          coalesce(halted_s, 0.0)             AS halted_s,
          coalesce(door_s, 0.0)               AS door_s,
          n_valid_obs,
          seg_segno = min(seg_segno) OVER w   AS first_used_seg,
          seg_segno = max(seg_segno) OVER w   AS last_used_seg
        FROM aggregates_by_seg
        WINDOW w AS (PARTITION BY jrnid)
      ),
      inserted AS (
        INSERT INTO %3$s (
          jrnid, segno, linkid, reversed, ij_dist_span,
          first_used_seg, last_used_seg,
          pt_timestamps, pt_seg_locs_m, pt_speeds_m_s, pt_doors,
          pt_obs_nums, pt_raw_offsets_m, pt_halt_offsets_m,
          halted_s, door_s, n_valid_obs
        )
        /*
         * Note how we use jrnid and segno here such that _all_ possible segments
         * of each pattern are included, not just the ones that happen to have
         * observation points projected on them. As a result, if a segment has got
         * no observations at all, its ag.xyz values are NULL. Such NULL segments later
         * get interpolated enter and exit ts values if they are between other segments
         * that have observations; or if the NULL segs before first or after last available
         * observations of a journey (e.g. the start of the journey has not followed its
         * planned itinerary), they are ultimately discarded as nothing can be interpolated
         * to them.
         */
        SELECT
          jr.jrnid, sg.segno, sg.linkid, sg.reversed, sg.ij_dist_span,
          ag.first_used_seg, ag.last_used_seg,
          ag.pt_timestamps, ag.pt_seg_locs_m, ag.pt_speeds_m_s, ag.pt_doors,
          ag.pt_obs_nums, ag.pt_raw_offsets_m, ag.pt_halt_offsets_m,
          ag.halted_s, ag.door_s, ag.n_valid_obs
        FROM %2$s                             AS jr
        INNER JOIN sched.segments             AS sg
          ON jr.ptid = sg.ptid
        LEFT JOIN aggr_with_end_segment_flags AS ag
          ON  ag.jrnid = jr.jrnid
          AND ag.seg_segno = sg.segno
        RETURNING 1
      )
    SELECT count(*) FROM inserted;
    $s$,
    jrn_point_table,
    journey_table,
    jrn_segs_table
  ) INTO cnt_ins;
  RETURN cnt_ins;
END;
$$;
COMMENT ON FUNCTION stage_hfp.extract_jrn_segs_from_jrn_points IS
'Collect and aggregate values from `jrn_point_table` by `jrnid` and `seg_segno`,
join `journey_table` and `sched.segments` to add all the scheduled segments
that have not necessarily got any observations but can later get interpolated values,
and insert results into `jrn_segs_table`.
This is the first step in modelling realized journey at segment level
after point-level operations.';

DROP FUNCTION IF EXISTS stage_hfp.set_seg_firstlast_values;
CREATE FUNCTION stage_hfp.set_seg_firstlast_values(
  jrn_segs_table    regclass,
  jrn_point_table   regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH updated AS (
      UPDATE %1$s AS upd
      SET
        fl_timestamps   = rng.fl_timestamps,
        fl_pt_abs_locs  = rng.fl_pt_abs_locs
      FROM (
        SELECT DISTINCT ON (jrnid, seg_segno)
        jrnid,
        seg_segno                   AS segno,
        ARRAY[
          first_value(tst) OVER w,
          last_value(tst) OVER w
        ]                           AS fl_timestamps,
        ARRAY[
          first_value(pt_abs_loc) OVER w,
          last_value(pt_abs_loc) OVER w
        ]                           AS fl_pt_abs_locs
        FROM %2$s
        WINDOW w AS (
          PARTITION BY jrnid, seg_segno
          ORDER BY obs_num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
      ) AS rng
      WHERE upd.jrnid = rng.jrnid
        AND upd.segno = rng.segno
      RETURNING *
    )
    SELECT count(*) FROM updated;
    $s$,
    jrn_segs_table,
    jrn_point_table
  ) INTO cnt_upd;
  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_seg_firstlast_values IS
'Update `fl_timestamps` and `fl_pt_abs_locs` fields in `jrn_segs_table`
with first and last `tst` and `pt_abs_loc` (by `obs_num` order) of each segment
in `jrn_point_table`. These values are used later to interpolate timestamp
values at start and end of each segment.';

DROP FUNCTION IF EXISTS stage_hfp.interpolate_enter_exit_ts;
CREATE FUNCTION stage_hfp.interpolate_enter_exit_ts(
  jrn_segs_table    regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd       bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
      interpolation_groups AS (
        SELECT
          jrnid,
          segno,
          lower(ij_dist_span)   AS enter_x,
          fl_timestamps[1]      AS first_ts,
          fl_timestamps[2]      AS last_ts,
          fl_pt_abs_locs[1]     AS first_x,
          fl_pt_abs_locs[2]     AS last_x,
          sum(
            CASE WHEN fl_timestamps IS NULL THEN 0 ELSE 1 END
          ) OVER w              AS ip_grp
        FROM %1$s
        WINDOW w AS (PARTITION BY jrnid ORDER BY segno)
        ORDER BY jrnid, segno
      ),
      grouped_vals AS (
        SELECT
          jrnid, ip_grp,
          min(first_ts)         AS grp_first_ts,
          min(last_ts)          AS grp_last_ts,
          min(first_x)          AS grp_first_x,
          min(last_x)           AS grp_last_x
        FROM interpolation_groups
        GROUP BY jrnid, ip_grp
      ),
      grouped_window_vals AS (
        SELECT
          jrnid, ip_grp,
          lag(grp_last_x)       OVER w  AS grp_x_0,
          lead(grp_first_x)     OVER w  AS grp_x_1,
          lag(grp_last_ts)      OVER w  AS grp_t_0,
          lead(grp_first_ts)    OVER w  AS grp_t_1
        FROM grouped_vals
        WINDOW w AS (PARTITION BY jrnid ORDER BY ip_grp)
      ),
      refs_prepared AS (
        SELECT
          ip.jrnid, ip.segno, ip.enter_x, ip.ip_grp,
          ip.first_ts, ip.last_ts,
          gr.grp_x_0                        AS x_0,
          coalesce(ip.first_x, gr.grp_x_1)  AS x_1,
          gr.grp_t_0                        AS t_0,
          coalesce(ip.first_ts, gr.grp_t_1) AS t_1
        FROM interpolation_groups   AS ip
        INNER JOIN grouped_window_vals  AS gr
          ON  ip.jrnid = gr.jrnid
          AND ip.ip_grp = gr.ip_grp
      ),
      interpolated AS (
        SELECT
          *,
          linear_interpolate(
            enter_x, x_0, t_0, x_1, t_1
          ) AS enter_ts
        FROM refs_prepared
      ),
      add_exits_replace_nulls AS (
        SELECT
          jrnid, segno,
          coalesce(enter_ts, first_ts)   AS enter_ts,
          coalesce(
            lead(enter_ts) OVER (PARTITION BY jrnid ORDER BY segno),
            last_ts
          ) AS exit_ts
        FROM interpolated
      ),
      updated AS (
        UPDATE %1$s AS upd
        SET
          enter_ts  = ip.enter_ts,
          exit_ts   = ip.exit_ts,
          thru_s    = extract(epoch FROM (ip.exit_ts - ip.enter_ts))
        FROM (SELECT * FROM add_exits_replace_nulls) AS ip
        WHERE upd.jrnid = ip.jrnid
          AND upd.segno = ip.segno
        RETURNING 1
      )
      SELECT count(*) FROM updated
    $s$,
    jrn_segs_table
  ) INTO cnt_upd;
  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.interpolate_enter_exit_ts IS
'Set `enter_ts` and `exit_ts` values in `jrn_segs_table` by interpolating:
- Segments with observations and with a preceding segment with obs
  get their `enter_ts` interpolated by the last ts & loc of prev segment
  and their own first ts & loc (normal case)
- Segments with NULL obs similarly but using the previous available last ts & loc
  and next available first ts & loc
- First segment (with observations) of a journey gets its `first_ts` as `enter_ts`,
  last segment gets its `last_ts` as `exit_ts` since these have no preceding /
  following segments as references, and therefore derived measures on these segments
  are NOT reliable! (enter-exit ts are however required for the db model.)
Also the derived `thru_s` value in seconds is set.';

DROP FUNCTION IF EXISTS stage_hfp.set_pt_timediffs_array;
CREATE FUNCTION stage_hfp.set_pt_timediffs_array(
  jrn_segs_table    regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd       bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
      unnested AS (
        SELECT
          jrnid,
          segno,
          unnest(pt_obs_nums) AS obs_num,
          extract(
            epoch FROM unnest(pt_timestamps) - enter_ts
          )::real             AS pt_timediff_s
        FROM %1$s
        WHERE enter_ts IS NOT NULL
      ),
      updated AS (
        UPDATE %1$s AS upd
        SET pt_timediffs_s  = td.pt_timediffs_s
        FROM (
          SELECT
            jrnid,
            segno,
            array_agg(pt_timediff_s ORDER BY obs_num) AS pt_timediffs_s
          FROM unnested
          GROUP BY jrnid, segno
        ) AS td
        WHERE upd.jrnid = td.jrnid
          AND upd.segno = td.segno
        RETURNING 1
      )
      SELECT count(*) FROM updated
    $s$,
    jrn_segs_table
  ) INTO cnt_upd;
  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_pt_timediffs_array IS
'Populate `pt_timediffs_s` array field in `jrn_segs_table`
using `enter_ts` and `pt_timestamps`.';
