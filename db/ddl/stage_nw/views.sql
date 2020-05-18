CREATE OR REPLACE VIEW stage_nw.view_trip_template_segments AS (
  WITH
    unnested AS (
      SELECT
        ttid,
        route_id                AS route,
        direction_id + 1        AS dir,
        unnest(stop_ids)        AS stop_id,
        unnest(stop_sequences)  AS stop_seq,
        route_found
      FROM stage_gtfs.trip_template_arrays
    )
  SELECT
    un.*,
    tr.path_seq,
    li.linkid,
    li.geom
  FROM unnested                           AS un
  LEFT JOIN stage_nw.trip_template_routes AS tr
    ON un.ttid = tr.ttid AND un.stop_seq = tr.stop_seq
  LEFT JOIN nw.links                      AS li
    ON tr.edge = li.linkid
);

CREATE OR REPLACE VIEW stage_nw.view_nodepairs_not_routed AS (
  SELECT
    sn.i_node                       AS i,
    sn.j_node                       AS j,
    ST_MakeLine(ind.geom, jnd.geom) AS geom
  FROM stage_nw.successive_nodes  AS sn
  INNER JOIN nw.nodes             AS ind
    ON sn.i_node = ind.nodeid
  INNER JOIN nw.nodes             AS jnd
    ON sn.j_node = jnd.nodeid
  WHERE sn.routed IS false
);
