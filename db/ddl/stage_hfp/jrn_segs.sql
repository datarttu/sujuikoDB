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
