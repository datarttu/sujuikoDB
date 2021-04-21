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
  candidate_locs    double precision[],
  seg_segno         smallint,         -- ref sched.segments
  seg_linkid        integer,          -- -""-
  seg_reversed      boolean,          -- -""-
  seg_rel_loc       double precision, -- linear loc along seg 0 ... 1
  seg_abs_loc       double precision, -- -""- but absolute 0 ... <seg_length>
  pt_abs_loc        double precision, -- -""- but absolute 0 ... <seg_length>
  pt_dx             double precision,
  duration_s        double precision,
  pt_spd            double precision,
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
        array_agg(sg.hdg ORDER BY sg.dist)    AS candidate_hdgs,
        array_agg(sg.loc ORDER BY sg.dist)    AS candidate_locs
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
          CASE WHEN seg.reversed IS true THEN
            1 - ST_LineLocatePoint(l.geom, pt.geom)
          ELSE ST_LineLocatePoint(l.geom, pt.geom)
          END                           AS loc,
          ST_Distance(pt.geom, l.geom)  AS dist
        FROM sched.segments AS seg
        INNER JOIN nw.links AS l
          ON seg.linkid = l.linkid
        WHERE seg.ptid = pt.ptid
          AND ST_DWithin(pt.geom, l.geom, %2$s)
          -- AND ST_Intersects(
          --   pt.geom,
          --   ST_Buffer(l.geom, %2$s, 'endcap=flat')
          -- )
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
        candidate_hdgs  = cd.candidate_hdgs,
        candidate_locs  = cd.candidate_locs
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
          (loc > 0 AND loc < 1)                   AS not_at_seg_end,
          dist
        FROM unnest(
          rec.seg_candidates,
          rec.candidate_hdgs,
          rec.candidate_locs,
          rec.candidate_dists
        ) AS s(cand, hdg, loc, dist)
      )
      SELECT INTO current_segno cand
      FROM criteria
      ORDER BY
        has_small_diff  DESC,
        not_at_seg_end  DESC,
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
2)  if multiple choices after prio. 1, choose the segment that would have the
    raw point projected somewhere in the middle rather than extreme ends
    (linear loc 0 or 1) of the segment geometry - this way we prefer projecting
    points from the sides of the segments;
3)  if multiple choices after prio. 2, choose the segment whose heading difference
    to the point heading is less than 90 degrees - this should take care of most
    "stich" segments where the next segment is immediately on the same link but
    in opposite direction;
4)  if still conflicts after prio. 3, prefer shortest distance between segment
    and point.
NOTE: This algorithm does NOT work well if
      - the segment geometry is very curvy and the angle from `ST_Azimuth()`
        does not thus represent the segment well enough, making comparison between
        point and segment heading pretty pointless;
      - the vehicle has really driven backwards (rare) or there are backwards
        jumping GPS points left still after movement value based filtering';

DROP FUNCTION IF EXISTS stage_hfp.discard_failed_seg_matches;
CREATE FUNCTION stage_hfp.discard_failed_seg_matches(
  jrn_point_table regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  this_rec          record;
  last_ok_rec       record;
  this_jrnid        uuid;
  this_min_segno    smallint;
  cnt_del           bigint;
  cnt_total         bigint;
BEGIN
  EXECUTE format(
    $s$
    SELECT count(DISTINCT jrnid) FROM %1$s
    $s$,
    jrn_point_table
  ) INTO cnt_total;

  CREATE TEMP TABLE _to_delete (
    jrnid     uuid,
    obs_range int8range
  )
  ON COMMIT DROP;

  EXECUTE format(
    $s$
    CREATE TEMP TABLE _obs_ranges ON COMMIT DROP AS (
      WITH
        segment_changes_marked AS (
          SELECT
            jrnid, obs_num, seg_segno,
            CASE WHEN seg_segno IS DISTINCT FROM lag(seg_segno) OVER w
              THEN 1 ELSE 0
            END AS segno_changed
          FROM %1$s
          WINDOW w AS (PARTITION BY jrnid ORDER BY obs_num)
        ),
        segment_visit_groups AS (
          SELECT
            jrnid, obs_num, seg_segno,
            sum(segno_changed) OVER w   AS segno_grp
          FROM segment_changes_marked
          WINDOW w AS (PARTITION BY jrnid ORDER BY obs_num)
        ),
        obs_ranges AS (
          SELECT
            jrnid, segno_grp, seg_segno,
            int8range(min(obs_num), max(obs_num), '[]') AS obs_range,
            count(*)                                    AS obs_count
          FROM segment_visit_groups
          GROUP BY jrnid, segno_grp, seg_segno
        )
      SELECT
        *,
        dense_rank() OVER (
          PARTITION BY jrnid, seg_segno ORDER BY obs_count DESC
        ) AS grp_prio
      FROM obs_ranges
      ORDER BY jrnid, segno_grp
    )
    $s$,
    jrn_point_table
  );

  -- Immediately drop cases that are clearly OK
  WITH deleted AS (
    DELETE FROM _obs_ranges AS obr
    USING (
      SELECT jrnid, max(grp_prio)
      FROM _obs_ranges
      GROUP BY jrnid
      HAVING max(grp_prio) = 1
    ) AS ok
    WHERE obr.jrnid = ok.jrnid
    RETURNING obr.jrnid
  )
  SELECT INTO cnt_del count(DISTINCT jrnid)
  FROM deleted;
  RAISE NOTICE '% / % journeys OK (no repeated segments)',
    cnt_del, cnt_total;
  IF cnt_del = cnt_total THEN
    RETURN 0;
  END IF;

  CREATE TEMP TABLE _min_segs_by_jrnid ON COMMIT DROP AS (
    SELECT jrnid, min(seg_segno) AS min_segno
    FROM _obs_ranges
    GROUP BY jrnid
  );

  FOR this_rec IN
    SELECT *
    FROM _obs_ranges
    ORDER BY jrnid, segno_grp
  LOOP

    IF this_rec.jrnid IS DISTINCT FROM this_jrnid THEN
      this_jrnid := this_rec.jrnid;
      last_ok_rec := NULL;
      -- On entering new jrnid, we should be at its minimum seg number.
      SELECT INTO this_min_segno min_segno
      FROM _min_segs_by_jrnid
      WHERE jrnid = this_jrnid;
    END IF;

    -- This holds true as long as not yet entered first valid rec of jrn.
    IF last_ok_rec IS NULL THEN

      IF this_rec.seg_segno > this_min_segno THEN
        INSERT INTO _to_delete
        VALUES (this_rec.jrnid, this_rec.obs_range);
      ELSE
        last_ok_rec := this_rec;
      END IF;

    ELSE
    -- Here we have already visited first valid rec of jrn
    -- so we start checking if segno increases & priorities.
      IF (this_rec.seg_segno > last_ok_rec.seg_segno AND this_rec.grp_prio = 1)
      OR this_rec.seg_segno = last_ok_rec.seg_segno
      THEN
        last_ok_rec := this_rec;
      ELSE
        -- 1) Backwards-going segments are discarded in any case
        -- 2) Segno can increase but still it can be a "jump" too far away
        --    forwards; *probably* this "jump" has priority nr > 2, meaning that
        --    there is a better matching group with that seg coming later.
        INSERT INTO _to_delete
        VALUES (this_rec.jrnid, this_rec.obs_range);
      END IF;

    END IF;

  END LOOP;

  DROP TABLE _min_segs_by_jrnid;

  EXECUTE format(
    $s$
    WITH deleted AS (
      DELETE FROM %1$s  AS jp
      USING _to_delete  AS td
      WHERE jp.jrnid = td.jrnid
        AND jp.obs_num <@ td.obs_range
      RETURNING *)
    SELECT count(*) FROM deleted
    $s$,
    jrn_point_table
  ) INTO cnt_del;

  DROP TABLE _to_delete;

  RETURN cnt_del;
END;
$$;
COMMENT ON FUNCTION stage_hfp.discard_failed_seg_matches IS
'Delete from `jrn_points_table` records where the segment matching has failed,
i.e. segment number does not increase correctly along timestamps:
- first points of the journey that are matched somewhere else than the least
  segment number occurring in the journey
  (e.g. 2, 2, 2 and only then 1, 1, 2, 3 ..., so the first 2, 2, 2 are dropped);
- points causing that a segment visited more than once, leaving points on other
  segments in between; in this case the largest group of successive points on
  the same segment is prioritized and other points are discarded.
  Points to delete are examined "on the fly", i.e., from segments occurring like
  1, 1, 2, 3, 2, 3, 3, 4, ... we only want to delete the last `2`, resulting in
  1, 1, 2, 3, 3, 3, 4, ...';

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

DROP FUNCTION IF EXISTS stage_hfp.set_linear_movement_values;
CREATE FUNCTION stage_hfp.set_linear_movement_values(
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
      deltas AS (
        SELECT
          jrnid,
          obs_num,
          coalesce(pt_abs_loc - lag(pt_abs_loc) OVER w, 0.0)  AS dx_m,
          extract(
            epoch FROM (lead(tst) OVER w - tst)
          )::double precision                                 AS duration_s,
          coalesce(
            (pt_abs_loc - lag(pt_abs_loc) OVER w) /
            extract(epoch FROM (tst - lag(tst) OVER w))::double precision,
            0.0
          )                                                   AS spd_m_s
        FROM %1$s
        WINDOW w AS (PARTITION BY jrnid ORDER BY obs_num)
      ),
      updated AS (
        UPDATE %1$s AS upd
        SET
          pt_dx       = d.dx_m,
          duration_s  = d.duration_s,
          pt_spd      = d.spd_m_s
        FROM (SELECT * FROM deltas) AS d
        WHERE upd.jrnid = d.jrnid
          AND upd.obs_num = d.obs_num
        RETURNING *
      )
    SELECT count(*) FROM updated
    $s$,
    jrn_point_table
  ) INTO cnt_upd;
  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_linear_movement_values IS
'Set `pt_dx`, `duration_s` and `pt_spd` values in `jrn_point_table`
based on `pt_abs_loc` and `tst` values of successive records.
Note that loc and speed values are respective to the previous point (lag)
but duration is defined respective to the next point.';
