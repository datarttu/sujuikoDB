EXPLAIN ANALYZE
SELECT
  jp.jrnid,
  jp.tst,
  sg.linkid,
  sg.reversed,
  ST_LineLocatePoint(sg.geom, jp.geom)  AS loc
FROM stage_hfp.journey_points AS jp
INNER JOIN stage_hfp.journeys AS jr
  ON jp.jrnid = jr.jrnid
LEFT JOIN LATERAL (
  SELECT seg.linkid, seg.reversed, seg.geom
  FROM sched.view_segment_geoms AS seg
  WHERE seg.ttid = jr.ttid
  ORDER BY seg.geom <-> jp.geom
  LIMIT 1
) AS sg
  ON true;

EXPLAIN ANALYZE
SELECT
  jp.jrnid,
  jp.tst,
  li.linkid,
  ST_LineLocatePoint(li.geom, jp.geom)  AS loc
FROM stage_hfp.journey_points AS jp
INNER JOIN stage_hfp.journeys AS jr
  ON jp.jrnid = jr.jrnid
LEFT JOIN LATERAL (
  SELECT lin.linkid, lin.geom
  FROM nw.links AS lin
  ORDER BY lin.geom <-> jp.geom
  LIMIT 1
) AS li
  ON true;
