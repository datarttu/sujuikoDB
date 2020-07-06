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

  seg_segno         smallint,         -- ref sched.segments
  seg_linkid        integer,          -- -""-
  seg_reversed      boolean,          -- -""-
  seg_rel_loc       double precision, -- linear loc along seg 0 ... 1
  seg_abs_loc       double precision, -- -""- but absolute 0 ... <seg_length>
  pt_rel_loc        double precision, -- linear loc along pattern geom 0 ... 1
  pt_abs_loc        double precision, -- -""- but absolute 0 ... <seg_length>
  raw_offset        double precision, -- distance raw <-> ref point on seg
  halted_push       double precision, -- how much pushed / pulled (-) along seg when clustering halted points by odo value

  is_redundant      boolean,
  n_rdnt_after      integer

  PRIMARY KEY (jrnid, obs_num)
);
COMMENT ON TABLE stage_hfp.jrn_points IS
'Points from `stage_hfp.raw` or corresponding temp table
whose `jrnid` represents a valid journey in `stage_hfp.journeys` (or corresp.)
and whose location is within a valid range from the journey shape
(defined when importing points).
Matching points to the trip segments is done using this table.';

CREATE INDEX ON stage_hfp.jrn_points USING GIST(geom_raw);

DROP FUNCTION IF EXISTS stage_hfp.insert_to_journey_points_from_raw;
CREATE OR REPLACE FUNCTION stage_hfp.insert_to_journey_points_from_raw(
  max_gps_tolerance   double precision,
  within_segment      double precision
)
RETURNS TABLE (table_name text, rows_inserted bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY
  WITH
    ongoing_vp_valid AS (
      SELECT r.jrnid, r.tst, r.odo, r.drst, r.stop, r.geom
      FROM stage_hfp.raw            AS r
      INNER JOIN stage_hfp.journeys AS j
        ON r.jrnid = j.jrnid
      WHERE r.is_ongoing IS true
        AND r.event_type = 'VP'
        AND cardinality(j.invalid_reasons) = 0
        AND r.geom IS NOT NULL
        AND r.odo IS NOT NULL
    ),
    mark_significant_observations AS (
      SELECT
        jrnid, tst, odo, drst, stop, geom,
        row_number() OVER (PARTITION BY jrnid ORDER BY tst) AS obs_num,
        CASE WHEN (
              odo   IS DISTINCT FROM lag(odo)   OVER w_tst
          OR  odo   IS DISTINCT FROM lead(odo)  OVER w_tst
          OR  drst  IS DISTINCT FROM lag(drst)  OVER w_tst
          OR  drst  IS DISTINCT FROM lead(drst) OVER w_tst
          OR  ST_Distance(geom, lag(geom) OVER w_tst) > max_gps_tolerance
          OR  ST_Distance(geom, lead(geom) OVER w_tst) > max_gps_tolerance
          ) THEN true
          ELSE false
        END AS is_significant
      FROM ongoing_vp_valid
      WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    ),
    journey_points AS (
      SELECT DISTINCT ON (jrnid, tst)
        jrnid,
        tst,
        odo,
        drst,
        stop,
        geom,
        obs_num,
        percent_rank() OVER (PARTITION BY jrnid ORDER BY tst) AS rel_rank
      FROM mark_significant_observations
      WHERE is_significant IS true
    ),
    segs_with_relranks AS (
      SELECT
        sg.ttid,
        sg.linkid,
        sg.i_node,
        sg.j_node,
        sg.i_rel_dist,
        sg.j_rel_dist,
        ST_Length(tot.geom)     AS total_cost,
        percent_rank() OVER (
          PARTITION BY sg.ttid
          ORDER BY sg.i_time
        )                       AS seg_rel_rank,
        li.geom                 AS seg_geom,
        li.reversed,
        li.cost
      FROM sched.segments                     AS sg
      INNER JOIN nw.view_links_with_reverses  AS li
        ON sg.linkid = li.linkid
        AND sg.i_node = li.inode
        AND sg.j_node = li.jnode
      INNER JOIN sched.mw_trip_template_geoms AS tot
        ON sg.ttid = tot.ttid
      INNER JOIN stage_hfp.journeys           AS jr
        ON sg.ttid = jr.ttid
      WHERE cardinality(jr.invalid_reasons) = 0
    ),
    points_with_seg_refs AS (
      SELECT
        jp.jrnid,
        jp.tst,
        jp.odo,
        jp.drst,
        jp.stop,
        jp.geom,
        jp.obs_num,
        jp.rel_rank,
        sg.linkid                                 AS seg_linkid,
        sg.reversed                               AS seg_reversed,
        ST_Distance(jp.geom, sg.seg_geom)         AS seg_offset,
        ST_LineLocatePoint(sg.seg_geom, jp.geom)  AS seg_rel_loc,
        sg.i_rel_dist,
        sg.j_rel_dist,
        sg.total_cost,
        sg.cost
      FROM journey_points AS jp
      INNER JOIN stage_hfp.journeys AS jrn
        ON jp.jrnid = jrn.jrnid
      LEFT JOIN LATERAL (
          SELECT swr.*
          FROM segs_with_relranks AS swr
          WHERE swr.ttid = jrn.ttid
            AND ST_DWithin(jp.geom, swr.seg_geom, within_segment)
          ORDER BY
            jp.geom <-> swr.seg_geom
            , abs(jp.rel_rank - swr.seg_rel_rank)
          LIMIT 1
        ) AS sg
        ON true
    ),
    points_with_abs_values AS (
      SELECT
        *,
        cost * seg_rel_loc                                    AS seg_abs_loc,
        i_rel_dist + (j_rel_dist - i_rel_dist) * seg_rel_loc  AS rel_dist,
        total_cost * i_rel_dist + cost * seg_rel_loc          AS abs_dist
      FROM points_with_seg_refs
    ),
    inserted AS (
      INSERT INTO stage_hfp.journey_points (
        jrnid, tst, odo, drst, stop, geom, obs_num, rel_rank,
        seg_linkid, seg_reversed, seg_offset,
        seg_rel_loc, seg_abs_loc,
        rel_dist, abs_dist,
        d_odo_ahead, dx_ahead, dt_ahead
      )
      SELECT
        jrnid, tst, odo, drst, stop, geom, obs_num, rel_rank,
        seg_linkid, seg_reversed, seg_offset,
        seg_rel_loc, seg_abs_loc,
        rel_dist, abs_dist,
        (lead(odo) OVER w_tst - odo)::real    AS d_odo_ahead,
        lead(abs_dist) OVER w_tst - abs_dist  AS dx_ahead,
        extract(
          epoch FROM (lead(tst) OVER w_tst - tst)
        )                                     AS dt_ahead
      FROM points_with_abs_values
      WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
      ORDER BY jrnid, tst

      RETURNING *
    )
  SELECT 'journey_points', count(*)
  FROM inserted;
END;
$$;

DROP FUNCTION IF EXISTS stage_hfp.set_journey_points_segment_vals;
CREATE OR REPLACE FUNCTION stage_hfp.set_journey_points_segment_vals(
  search_distance double precision
)
RETURNS TABLE (table_name text, rows_affected bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY

  WITH
    segs_with_relranks AS (
      SELECT
        sg.ttid,
        sg.linkid,
        sg.i_node,
        sg.j_node,
        sg.i_rel_dist,
        sg.j_rel_dist,
        percent_rank() OVER (
          PARTITION BY sg.ttid
          ORDER BY sg.i_time
        )                       AS seg_rel_rank,
        li.geom                 AS seg_geom,
        li.reversed
      FROM sched.segments                     AS sg
      INNER JOIN nw.view_links_with_reverses  AS li
        ON sg.linkid = li.linkid
        AND sg.i_node = li.inode
        AND sg.j_node = li.jnode
      INNER JOIN stage_hfp.journeys           AS jr
        ON sg.ttid = jr.ttid
      WHERE cardinality(jr.invalid_reasons) = 0
    ),
    journey_points_to_update AS (
      SELECT
        jp.jrnid,
        jp.tst,
        jp.sub_id,
        sg.linkid,
        sg.reversed,
        sg.i_rel_dist,
        sg.j_rel_dist,
        ST_Distance(jp.geom, sg.seg_geom)         AS seg_offset,
        ST_LineLocatePoint(sg.seg_geom, jp.geom)  AS seg_rel_loc
      FROM stage_hfp.journey_points AS jp
      INNER JOIN stage_hfp.journeys AS jrn
        ON jp.jrnid = jrn.jrnid
      INNER JOIN LATERAL (
          SELECT swr.*
          FROM segs_with_relranks AS swr
          WHERE swr.ttid = jrn.ttid
            AND ST_DWithin(jp.geom, swr.seg_geom, search_distance)
          ORDER BY
            jp.geom <-> swr.seg_geom,
            abs(jp.rel_rank - swr.seg_rel_rank)
          LIMIT 1
        ) AS sg
        ON true
    ),
    updated AS (
      UPDATE stage_hfp.journey_points AS upd
      SET
        seg_linkid    = tu.linkid,
        seg_reversed  = tu.reversed,
        seg_offset    = tu.seg_offset,
        seg_rel_loc   = tu.seg_rel_loc,
        rel_dist      = tu.i_rel_dist + tu.seg_rel_loc * (tu.j_rel_dist - tu.i_rel_dist)
      FROM (
        SELECT * FROM journey_points_to_update
      ) AS tu
      WHERE upd.jrnid   = tu.jrnid
        AND upd.tst     = tu.tst
        AND upd.sub_id  = tu.sub_id
      RETURNING *
    )
  SELECT 'journey_points', count(*)
  FROM updated;
END;
$$;
