BEGIN;

DROP TABLE IF EXISTS stage_hfp.test_projected;

CREATE TABLE stage_hfp.test_projected AS (
WITH
  test_raw AS (
    SELECT *
    FROM stage_hfp.raw
    WHERE event_type = 'VP'
      AND is_ongoing IS true
      AND route = '1088'
  ),
  jrn AS (
    SELECT DISTINCT ON (r.jrnid, r.oday, r.start, r.route, r.dir)
      r.jrnid, r.oday, r.start, r.route, r.dir,
      vt.ttid
    FROM test_raw               AS r
    LEFT JOIN sched.view_trips AS vt
      ON r.oday = vt.service_date
      AND r.start = vt.start_time
      AND r.route = vt.route
      AND r.dir = vt.dir
  )
  ,
  jrn_segments AS (
    SELECT
      jrn.*,
      vsg.linkid,
      vsg.i_node,
      vsg.j_node,
      vsg.i_time,
      vsg.reversed,
      vsg.geom,
      vsg.cost,
      vsg.i_cumul_cost,
      row_number() OVER (PARTITION BY jrn.jrnid ORDER BY vsg.i_time)  AS ord
    FROM jrn                            AS jrn
    INNER JOIN sched.view_segment_geoms AS vsg
      ON jrn.ttid = vsg.ttid
    ORDER BY jrn.jrnid, vsg.i_time
  )
  ,
  raw_points AS (
    SELECT
      jrnid,
      row_number() OVER w_tst_event         AS obs,
      tst,
      odo,
      drst,
      stop,
      geom,

      (tst - lag(tst) OVER w_tst_event)     AS dt_prev,
      (lead(tst) OVER w_tst_event - tst)    AS dt_next,
      ST_Distance(
        geom, lag(geom) OVER w_tst_event)   AS dx_prev,
      ST_Distance(
        lead(geom) OVER w_tst_event, geom)  AS dx_next,
      lag(drst) OVER w_tst_event            AS drst_prev,
      lead(drst) OVER w_tst_event           AS drst_next,
      lag(stop) OVER w_tst_event            AS stop_prev,
      lead(stop) OVER w_tst_event           AS stop_next
    FROM test_raw
    WINDOW w_tst_event AS (PARTITION BY jrnid ORDER BY tst)
    ORDER BY jrnid, tst
  )
  ,
  drop_repeated AS (
    SELECT
      jrnid, obs, tst, odo, drst, stop, dt_prev, dx_prev, geom
    FROM raw_points
    WHERE
      drst IS DISTINCT FROM drst_prev OR drst IS DISTINCT FROM drst_next
      OR stop IS DISTINCT FROM stop_prev OR stop IS DISTINCT FROM stop_next
      OR dx_prev > 0.5 OR dx_next > 0.5
  )
  ,
  points_on_segments AS (
    SELECT
      pt.jrnid, pt.obs, pt.tst, pt.odo, pt.drst, pt.stop,
      pt.geom AS pt_geom,
      sg.route, sg.dir, sg.start,
      sg.ord AS seg_ord,
      sg.linkid, sg.reversed, sg.cost, sg.i_cumul_cost,
      sg.geom AS sg_geom
    FROM drop_repeated  AS pt
    LEFT JOIN LATERAL (
      SELECT *
      FROM jrn_segments AS jsg
      WHERE jsg.jrnid = pt.jrnid
      ORDER BY jsg.geom <-> pt.geom
      LIMIT 1
    ) AS sg
    ON true
  )
  ,
  projected_geoms AS (
    SELECT
      jrnid, route, dir, start, obs, tst, odo, drst, stop, seg_ord, linkid, reversed, cost, i_cumul_cost,
      pt_geom,
      ST_Distance(sg_geom, pt_geom)         AS offset_dist,
      ST_ClosestPoint(sg_geom, pt_geom)     AS ref_geom,
      ST_ShortestLine(sg_geom, pt_geom)     AS ln_geom,
      ST_LineLocatePoint(sg_geom, pt_geom) * cost + i_cumul_cost AS ref_cumul_loc
    FROM points_on_segments
    ORDER BY jrnid, tst
  )
  ,
  projected_deltas AS (
    SELECT
      *,
      lead(ref_cumul_loc) OVER w_tst - ref_cumul_loc    AS pt_dx_m,
      extract(epoch FROM (lead(tst) OVER w_tst - tst))  AS pt_dt_s
    FROM projected_geoms
    WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    ORDER BY jrnid, tst
  )
  SELECT
    *,
    CASE WHEN pt_dt_s = 0 THEN 0
    ELSE pt_dx_m / pt_dt_s END AS pt_v_ms
  FROM projected_deltas
  ORDER BY jrnid, tst
);

ALTER TABLE stage_hfp.test_projected
ADD PRIMARY KEY (jrnid, obs);

SAVEPOINT test_projected_made;

CREATE INDEX ON stage_hfp.test_projected (jrnid, seg_ord, linkid);
CREATE INDEX ON stage_hfp.test_projected USING GIST (pt_geom);
CREATE INDEX ON stage_hfp.test_projected USING GIST (ref_geom);
CREATE INDEX ON stage_hfp.test_projected USING GIST (ln_geom);

DROP TABLE IF EXISTS stage_hfp.test_seg_aggr;
CREATE TABLE stage_hfp.test_seg_aggr AS (
  WITH aggregates AS (
    SELECT
      jrnid,
      seg_ord,
      linkid,
      bool_or(reversed) AS reversed,
      min(ref_cumul_loc)  AS loc_min,
      max(ref_cumul_loc)  AS loc_max,
      min(tst)  AS tst_min,
      max(tst)  AS tst_max,
      sum(pt_dt_s) filter(WHERE pt_dx_m BETWEEN -0.5 AND 0.5) AS stopped_s,
      sum(pt_dt_s) filter(WHERE drst IS true) AS dropen_s,
      count(*)  AS n_pts
    FROM stage_hfp.test_projected
    GROUP BY jrnid, seg_ord, linkid
    ORDER BY jrnid, seg_ord
  ),
  agg_geom AS (
    SELECT
      ag.*,
      ag.loc_max - ag.loc_min                     AS thru_dist,
      extract(epoch FROM (ag.tst_max-ag.tst_min)) AS thru_seconds
    FROM aggregates     AS ag
    ORDER BY ag.jrnid, ag.seg_ord
  )
  SELECT *,
    CASE WHEN thru_seconds = 0 OR thru_dist = 0 THEN 0
    ELSE thru_dist / thru_seconds * 3.6 END  AS avg_kmh
  FROM agg_geom
);
ALTER TABLE stage_hfp.test_seg_aggr
ADD PRIMARY KEY (jrnid, seg_ord, linkid);
