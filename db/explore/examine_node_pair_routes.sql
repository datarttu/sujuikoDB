/*
 * Try to find node pair routes that are too long
 * and therefore possibly failed:
 * compare Euclidean and network distance.
 * E.g., a stop might have been projected on a wrong link.
 */

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS stage_nw.mat_pair_route_distances;

CREATE MATERIALIZED VIEW stage_nw.mat_pair_route_distances AS (
  WITH
    network_distances AS (
      SELECT
        npr.start_node                            AS i,
        npr.end_node                              AS j,
        array_agg(npr.edge ORDER BY npr.path_seq) AS edges,
        sum(ln.cost)                              AS nw_dist
      FROM stage_nw.node_pair_routes  AS npr
      INNER JOIN nw.links             AS ln
        ON npr.edge = ln.linkid
      GROUP BY i, j
    ),
    euclidean_distances AS (
      SELECT DISTINCT ON (npr.start_node, npr.end_node)
        npr.start_node                  AS i,
        npr.end_node                    AS j,
        ST_Distance(nd1.geom, nd2.geom) AS eu_dist,
        ST_MakeLine(nd1.geom, nd2.geom) AS geom
      FROM stage_nw.node_pair_routes  AS npr
      INNER JOIN nw.nodes             AS nd1
        ON npr.start_node = nd1.nodeid
      INNER JOIN nw.nodes             AS nd2
        ON npr.end_node = nd2.nodeid
    ),
    stops_per_node AS (
      SELECT
        nodeid,
        array_agg(stopid ORDER BY stopid) AS stopids
      FROM nw.stops
      GROUP BY nodeid
    )
  SELECT
    nwd.i,
    sn1.stopids               AS i_stops,
    nwd.j,
    sn2.stopids               AS j_stops,
    nwd.edges,
    nwd.mode,
    nwd.nw_dist,
    eud.eu_dist,
    nwd.nw_dist / eud.eu_dist AS dist_coeff,
    eud.geom
  FROM network_distances          AS nwd
  INNER JOIN stops_per_node       AS sn1
    ON nwd.i = sn1.nodeid
  INNER JOIN stops_per_node       AS sn2
    ON nwd.j = sn2.nodeid
  INNER JOIN euclidean_distances  AS eud
    ON nwd.i = eud.i AND nwd.j = eud.j
  ORDER BY dist_coeff DESC
);
