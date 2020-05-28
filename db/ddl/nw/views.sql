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
COMMENT ON VIEW nw.view_links_with_reverses IS
'All nw.links records plus separate records for two-way links,
created by inverting inode and jnode, cost and rcost, and geoms.
Records should be unique and thus safely joinable on
by linkid, inode and jnode.';

DROP VIEW IF EXISTS nw.view_stop_nodes;
CREATE VIEW nw.view_stop_nodes AS (
  SELECT
    st.*,
    nd.geom
  FROM nw.stops       AS st
  INNER JOIN nw.nodes AS nd
    ON st.nodeid = nd.nodeid
);
