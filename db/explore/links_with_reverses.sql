BEGIN;

CREATE VIEW nw.view_links_with_reverses AS (
  SELECT
    linkid, inode, jnode, mode, cost, rcost, osm_data, geom, wgs_geom,
    false AS reversed
  FROM nw.links
  UNION
  SELECT
    linkid,
    jnode                 AS inode,
    inode                 AS jnode,
    mode,
    rcost                 AS cost,
    cost                  AS rcost,
    osm_data,
    ST_Reverse(geom)      AS geom,
    ST_Reverse(wgs_geom)  AS wgs_geom,
    true                  AS reversed
  FROM  nw.links
  WHERE rcost > -1
);

WITH tt_subset AS (
  SELECT ttid, linkid, i_node, j_node, i_time
  FROM sched.segments
  WHERE ttid = '2550_2_1'
)
SELECT
  tt.*,
  li.mode,
  li.cost,
  ST_Summary(li.geom),
  li.reversed
FROM tt_subset                        AS tt
LEFT JOIN nw.view_links_with_reverses AS li
  ON  tt.linkid = li.linkid
  AND tt.i_node = li.inode
  AND tt.j_node = li.jnode
LIMIT 20;

\qecho 'Count should be max 1:'
SELECT linkid, inode, jnode, count(*) AS cnt
FROM nw.view_links_with_reverses
GROUP BY linkid, inode, jnode
ORDER BY cnt DESC
LIMIT 10;

ROLLBACK;
