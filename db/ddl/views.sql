/*
 * NOTE:
 * Not to be referenced by master script that only creates base tables
 * and functions.
 * Some functions such as those containing pgRouting routines
 * create more tables, and they have to be run (not only created)
 * to enable some of these views to be created.
 */

/*
 * TODO: This view has to be created only after nw.nodes exists!
 */
CREATE VIEW stage_nw.node_pairs_not_routed_geom AS (
  SELECT a.i_node, a.j_node, ST_MakeLine(b.geom, c.geom) AS geom
  FROM stage_nw.successive_nodes AS a
  INNER JOIN nw.nodes AS b ON a.i_node = b.nodeid
  INNER JOIN nw.nodes AS c ON a.j_node = c.nodeid
  WHERE NOT a.routed
);
COMMENT ON VIEW stage_nw.node_pairs_not_routed_geom IS
'Failed node pairs that have not marked as successfully routed,
with line geometry for visualization.';
