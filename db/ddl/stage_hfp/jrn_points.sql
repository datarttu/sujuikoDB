DROP TABLE IF EXISTS stage_hfp.jrn_points;
CREATE TABLE stage_hfp.jrn_points (
  jrnid             uuid                  NOT NULL,
  obs_num           bigint                NOT NULL,
  tst               timestamptz           NOT NULL,
  odo               integer,
  drst              boolean,
  geom              geometry(POINT, 3067),
  dodo              double precision,
  dx                double precision,
  spd               double precision,
  acc               double precision,
  hdg               double precision,

  ptid              text,
  seg_candidates    smallint[],
  candidate_dists   double precision[],
  candidate_hdgs    double precision[],
  seg_segno         smallint,         -- ref sched.segments
  seg_linkid        integer,          -- -""-
  seg_reversed      boolean,          -- -""-
  seg_rel_loc       double precision, -- linear loc along seg 0 ... 1
  seg_abs_loc       double precision, -- -""- but absolute 0 ... <seg_length>
  pt_abs_loc        double precision, -- -""- but absolute 0 ... <seg_length>
  pt_dx             double precision,
  pt_spd            double precision,
  pt_acc            double precision,
  raw_offset        double precision, -- distance raw <-> ref point on seg
  halt_offset       double precision, -- how much point was possibly moved by halted point clustering

  is_redundant      boolean,
  n_rdnt_after      smallint,         -- n of redundant points dropped after this obs

  PRIMARY KEY (jrnid, obs_num)
);
COMMENT ON TABLE stage_hfp.jrn_points IS
'Points from `stage_hfp.raw` or corresponding temp table
whose `jrnid` represents a valid journey in `stage_hfp.journeys` (or corresp.)
and whose location is within a valid range from the journey shape
(defined when importing points).
Matching points to the trip segments is done using this table.';

CREATE INDEX ON stage_hfp.jrn_points USING GIST(geom);

DROP FUNCTION IF EXISTS stage_hfp.extract_jrn_points_from_raw;
CREATE OR REPLACE FUNCTION stage_hfp.extract_jrn_points_from_raw(
  raw_table       regclass,
  jrn_point_table regclass,
  journey_table   regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_ins   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH inserted AS (
      INSERT INTO %1$s (
        jrnid, obs_num, tst, odo, drst, geom,
        dodo, dx, spd, acc, hdg, ptid
      )
      SELECT
        r.jrnid,
        r.obs_num,
        r.tst,
        r.odo,
        r.drst,
        r.geom,
        r.dodo,
        r.dx,
        r.spd,
        r.acc,
        r.hdg,
        j.ptid
      FROM %2$s                           AS r
      INNER JOIN %3$s                     AS j
        ON r.jrnid = j.jrnid
      RETURNING *
    )
    SELECT count(*) FROM inserted
    $s$,
    jrn_point_table,
    raw_table,
    journey_table
  ) INTO cnt_ins;

  RETURN cnt_ins;
END;
$$;
COMMENT ON FUNCTION stage_hfp.extract_jrn_points_from_raw IS
'Extracts from `raw_table` to `jrn_point_table` points that
have a corresponding `jrnid` journey in `journey_table`.';

DROP FUNCTION IF EXISTS stage_hfp.set_segment_candidates;
CREATE FUNCTION stage_hfp.set_segment_candidates(
  jrn_point_table regclass,
  max_distance    numeric
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH candidates AS (
      SELECT
        pt.jrnid,
        pt.obs_num,
        array_agg(sg.segno ORDER BY sg.dist)  AS seg_candidates,
        array_agg(sg.dist ORDER BY sg.dist)   AS candidate_dists,
        array_agg(sg.hdg ORDER BY sg.dist)    AS candidate_hdgs
      FROM %1$s AS pt
      INNER JOIN LATERAL (
        SELECT
          seg.ptid,
          seg.segno,
          degrees(
            CASE WHEN seg.reversed IS true THEN
              ST_Azimuth(
                ST_EndPoint(l.geom),
                ST_StartPoint(l.geom)
              )
            ELSE
              ST_Azimuth(
                ST_StartPoint(l.geom),
                ST_EndPoint(l.geom)
              )
            END
          )                             AS hdg,
          ST_Distance(pt.geom, l.geom)  AS dist
        FROM sched.segments AS seg
        INNER JOIN nw.links AS l
          ON seg.linkid = l.linkid
        WHERE seg.ptid = pt.ptid
          AND ST_DWithin(pt.geom, l.geom, %2$s)
          AND ST_Intersects(
            pt.geom,
            ST_Buffer(l.geom, %2$s, 'endcap=flat')
          )
        ORDER BY dist
      ) AS sg
      ON true
      GROUP BY pt.jrnid, pt.obs_num
    ),
    updated AS (
      UPDATE %1$s AS upd
      SET
        seg_candidates  = cd.seg_candidates,
        candidate_dists = cd.candidate_dists,
        candidate_hdgs  = cd.candidate_hdgs
      FROM (
        SELECT * FROM candidates
      ) AS cd
      WHERE upd.jrnid = cd.jrnid
        AND upd.obs_num = cd.obs_num
      RETURNING *
    )
    SELECT count(*) FROM updated
    $s$,
    jrn_point_table,
    max_distance
  ) INTO cnt_upd;

  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_segment_candidates IS
'Update `jrn_point_table`.`seg_candidates`: from pattern `ptid` segments,
list the segment numbers `segno` of those that are within maximum `max_distance`
from the point and save to `seg_candidates` array such that the nearest
segment is listed first.';

DROP FUNCTION IF EXISTS stage_hfp.discard_outlier_points;
CREATE FUNCTION stage_hfp.discard_outlier_points(
  jrn_point_table regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_del   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH deleted AS (
      DELETE FROM %1$s
      WHERE seg_candidates IS NULL
        OR cardinality(seg_candidates) = 0
      RETURNING *
    )
    SELECT count(*) FROM deleted
    $s$,
    jrn_point_table
  ) INTO cnt_del;

  RETURN cnt_del;
END;
$$;
COMMENT ON FUNCTION stage_hfp.discard_outlier_points IS
'From `jrn_point_table`, delete rows that do not have any `seg_candidates`,
i.e. they lie too far away from any pattern segment.
The distance threshold is defined earlier in `.set_segment_candidates()`.';

DROP FUNCTION IF EXISTS stage_hfp.cluster_halted_points;
CREATE FUNCTION stage_hfp.cluster_halted_points(
  jrn_point_table regclass,
  min_clust_size  integer DEFAULT 2
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
      odogroups AS (
        SELECT
          jrnid,
          obs_num,
          geom                                          AS old_geom,
          rank() OVER (PARTITION BY jrnid ORDER BY odo) AS odogroup,
          count(*) OVER (PARTITION BY jrnid, odo)       AS n_in_odogroup
        FROM %1$s
      ),
      medianpoints AS (
        SELECT
          jrnid,
          odogroup,
          ST_GeometricMedian( ST_Collect(old_geom) )  AS median_geom
        FROM odogroups
        WHERE n_in_odogroup >= %2$s
        GROUP BY jrnid, odogroup
      ),
      result AS (
        SELECT
          o.jrnid,
          o.obs_num,
          ST_Distance(o.old_geom, m.median_geom)  AS halt_offset,
          m.median_geom
        FROM odogroups          AS o
        INNER JOIN medianpoints AS m
          ON o.jrnid = m.jrnid AND o.odogroup = m.odogroup
      ),
      updated AS (
        UPDATE %1$s AS upd
        SET
          geom = r.median_geom,
          halt_offset = r.halt_offset
        FROM (SELECT * FROM result) AS r
        WHERE upd.jrnid = r.jrnid
          AND upd.obs_num = r.obs_num
        RETURNING *
      )
      SELECT count(*) FROM updated
    $s$,
    jrn_point_table,
    min_clust_size
  ) INTO cnt_upd;

  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.cluster_halted_points IS
'In `jrn_point_table`, move multiple points having the same `odo` value
to the same location, if there are at least `min_clust_size` points
with the same `odo` value.
These points are considered "halted", i.e. the vehicle
is not moving, so we want to force them to the same location to get rid of
movement values caused by GPS jitter.';

DROP FUNCTION IF EXISTS stage_hfp.mark_redundant_jrn_points;
CREATE FUNCTION stage_hfp.mark_redundant_jrn_points(
  jrn_point_table regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_updated     bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
      window_refs AS (
        SELECT
          jrnid,
          obs_num,
          lag(odo) OVER w   AS prev_odo,
          odo,
          lead(odo) OVER w  AS next_odo,
          lag(drst) OVER w  AS prev_drst,
          drst,
          lead(drst) OVER w AS next_drst
        FROM %1$s
        WINDOW w AS (PARTITION BY jrnid ORDER BY obs_num)
      ),
      comparisons AS (
        SELECT
          jrnid,
          obs_num,
          NOT (
            prev_odo IS DISTINCT FROM odo
            OR next_odo IS DISTINCT FROM odo
            OR prev_drst IS DISTINCT FROM drst
            OR next_drst IS DISTINCT FROM drst
          ) AS is_redundant
        FROM window_refs
      ),
      group_beginnings AS (
        SELECT
          jrnid,
          obs_num,
          is_redundant,
          CASE WHEN is_redundant IS true
            AND lag(is_redundant) OVER (PARTITION BY jrnid ORDER BY obs_num) IS false
            THEN 1
          ELSE 0
          END AS new_group
        FROM comparisons
      ),
      groups AS (
        SELECT
          jrnid,
          obs_num,
          is_redundant,
          sum(new_group) OVER (PARTITION BY jrnid ORDER BY obs_num) AS grp
        FROM group_beginnings
      ),
      group_aggregates AS (
        SELECT
          jrnid,
          grp,
          count(*) filter(WHERE is_redundant IS true)         AS n_redundant,
          min(obs_num) filter(WHERE is_redundant IS true) - 1 AS last_sig_obs_num
        FROM groups
        GROUP BY jrnid, grp
      ),
      result AS (
        SELECT
          g.*,
          coalesce(ga.n_redundant, 0) AS n_rdnt_after
        FROM groups                 AS g
        LEFT JOIN group_aggregates  AS ga
          ON g.jrnid = ga.jrnid
          AND g.obs_num = ga.last_sig_obs_num
        ORDER BY g.jrnid, g.obs_num
      ),
      updated AS (
        UPDATE %1$s AS upd
        SET
          is_redundant = r.is_redundant,
          n_rdnt_after = r.n_rdnt_after
        FROM (SELECT * FROM result) AS r
        WHERE upd.jrnid = r.jrnid
          AND upd.obs_num = r.obs_num
        RETURNING *
      )
      SELECT count(*) FROM updated
    $s$,
    jrn_point_table
  ) INTO n_updated;

  RETURN n_updated;
END;
$$;
COMMENT ON FUNCTION stage_hfp.mark_redundant_jrn_points IS
'From `jrn_point_table`, mark `is_redundant` to true where
- `odo` value is same as on preceding and following row by `obs_num`
- `drst` does not change with respect to prec. and foll. row
I.e. movement or door status does not change on these rows.
Also populate `n_rdnt_after` for rows just before groups of
redundant values.';

DROP FUNCTION IF EXISTS stage_hfp.discard_redundant_jrn_points;
CREATE FUNCTION stage_hfp.discard_redundant_jrn_points(
  jrn_point_table regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_del   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH deleted AS (
      DELETE FROM %1$s
      WHERE is_redundant IS true
      RETURNING *
    )
    SELECT count(*) FROM deleted
    $s$,
    jrn_point_table
  ) INTO cnt_del;

  RETURN cnt_del;
END;
$$;
COMMENT ON FUNCTION stage_hfp.discard_redundant_jrn_points IS
'From `jrn_point_table`, delete rows marked with `is_redundant = true`.
Run this after `mark_redundant_jrn_points()` and possibly checking
that the results are ok.';

DROP FUNCTION IF EXISTS stage_hfp.set_best_match_segments;
CREATE FUNCTION stage_hfp.set_best_match_segments(
  jrn_point_table regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  rec             record;
  current_jrnid   uuid;
  current_segno   smallint;
  last_segno      smallint;
  cnt_upd         bigint;
BEGIN
  cnt_upd := 0;

  FOR rec IN EXECUTE format(
    $s$
    SELECT * FROM %1$s
    ORDER BY jrnid, obs_num
    $s$,
    jrn_point_table
  ) LOOP
    CONTINUE WHEN rec.seg_candidates IS NULL;

    IF current_jrnid IS DISTINCT FROM rec.jrnid THEN
      current_jrnid := rec.jrnid;
      SELECT INTO current_segno min(cand)
      FROM unnest(rec.seg_candidates) AS s(cand);
      last_segno := current_segno;

    ELSE
      WITH criteria AS (
        SELECT
          cand,
          (cand - last_segno BETWEEN 0 AND 1)     AS has_small_diff,
          (minimum_angle(hdg, rec.hdg) < 90.0)    AS has_small_angle,
          dist
        FROM unnest(
          rec.seg_candidates,
          rec.candidate_hdgs,
          rec.candidate_dists
        ) AS s(cand, hdg, dist)
      )
      SELECT INTO current_segno cand
      FROM criteria
      ORDER BY
        has_small_diff  DESC,
        has_small_angle DESC,
        dist            ASC
      LIMIT 1;

    END IF;

    EXECUTE format(
      $s$
      UPDATE %1$s
      SET seg_segno = %2$s
      WHERE jrnid = %3$L
        AND obs_num = %4$s
      $s$,
      jrn_point_table,
      quote_nullable(current_segno),
      current_jrnid,
      rec.obs_num
    );

    IF current_segno IS NOT NULL THEN
      last_segno := current_segno;
      cnt_upd := cnt_upd + 1;
    END IF;

  END LOOP;

  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_best_match_segments IS
'Set pattern segment references in `jrn_point_table` by using candidates
from `seg_candidates`, `candidate_dists` and `candidate_hdgs`.
If there are multiple candidates, they are prioritized as follows:
1)  candidates whose segment number is equal to or 1 greater than the latest
    segment number used: we do not want to go backwards along the segments;
2)  if multiple choices after prio. 1, choose the segment whose heading difference
    to the point heading is less than 90 degrees - this should take care of most
    "stich" segments where the next segment is immediately on the same link but
    in opposite direction;
3)  if still conflicts after prio. 2, prefer shortest distance between segment
    and point.
NOTE: This algorithm does NOT work well if
      - the segment geometry is very curvy and the angle from `ST_Azimuth()`
        does not thus represent the segment well enough, making comparison between
        point and segment heading pretty pointless;
      - the vehicle has really driven backwards (rare) or there are backwards
        jumping GPS points left still after movement value based filtering';

DROP FUNCTION IF EXISTS stage_hfp.set_linear_locations;
CREATE FUNCTION stage_hfp.set_linear_locations(
  jrn_point_table regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
      seg_attrs AS (
        SELECT
          jp.jrnid,
          jp.obs_num,
          sg.ptid,
          sg.linkid,
          sg.reversed,
          sg.ij_dist_span,
          ST_Distance(l.geom, jp.geom)        AS raw_offset,
          CASE WHEN sg.reversed THEN
            1 - ST_LineLocatePoint(l.geom, jp.geom)
          ELSE ST_LineLocatePoint(l.geom, jp.geom)
          END                                 AS seg_rel_loc,
          l.cost                              AS link_len
        FROM %1$s AS jp
        INNER JOIN sched.segments AS sg
          ON  jp.ptid = sg.ptid
          AND jp.seg_segno = sg.segno
        INNER JOIN nw.links       AS l
          ON sg.linkid = l.linkid
      ),
      linear_values AS (
        SELECT
          jrnid,
          obs_num,
          linkid                      AS seg_linkid,
          reversed                    AS seg_reversed,
          raw_offset,
          seg_rel_loc,
          seg_rel_loc * link_len      AS seg_abs_loc,
          lower(ij_dist_span) +
            (seg_rel_loc * link_len)  AS pt_abs_loc
        FROM seg_attrs
      ),
      updated AS (
        UPDATE %1$s AS upd
        SET
          seg_linkid    = r.seg_linkid,
          seg_reversed  = r.seg_reversed,
          seg_rel_loc   = r.seg_rel_loc,
          seg_abs_loc   = r.seg_abs_loc,
          pt_abs_loc    = r.pt_abs_loc,
          raw_offset    = r.raw_offset
        FROM (SELECT * FROM linear_values) AS r
        WHERE upd.jrnid = r.jrnid
          AND upd.obs_num = r.obs_num
        RETURNING *
      )
    SELECT count(*) FROM updated
    $s$,
    jrn_point_table
  ) INTO cnt_upd;

  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_linear_locations IS
'In `jrn_point_table`, project raw point geometries onto segments the points
belong to, and calculate both relative and absolute linear locations
along the segment and, derived from them, along the pattern geometry.
Also save the link id and reverse attribute used in the process for possible auditing.
Updates `seg_linkid`, `seg_reversed`, `seg_rel_loc`, `seg_abs_loc`,
`pt_rel_loc`, `raw_offset`.';
