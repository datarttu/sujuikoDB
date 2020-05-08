CREATE TABLE stage_nw.node_pair_routes (
  start_node  integer     NOT NULL,
  end_node    integer     NOT NULL,
  path_seq    integer     NOT NULL,
  node        integer     NOT NULL,
  edge        integer,
  PRIMARY KEY (start_node, end_node, path_seq)
);

CREATE OR REPLACE FUNCTION stage_nw.route_node_pairs()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       integer;
  n_routed  integer;
  n_total   integer;
  n_otm     integer;
  esql      text;
  route_rec record;
BEGIN
  DELETE FROM stage_nw.node_pair_routes;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_nw.node_pair_routes', cnt;

  UPDATE stage_nw.successive_nodes
  SET routed = false;

  SELECT INTO n_total count(*) FROM stage_nw.successive_nodes;
  SELECT INTO n_otm count(DISTINCT i_node)
  FROM stage_nw.successive_nodes;
  RAISE NOTICE 'Routing % unique node pairs as % one-to-many records ...', n_total, n_otm;

  -- "edges_sql" query used as pgr_Dijkstra function input.
  esql := '
  SELECT
    linkid  AS id,
    inode   AS source,
    jnode   AS target,
    cost,
    rcost   AS reverse_cost
  FROM nw.links';

  n_routed := 0;

  FOR route_rec IN
    SELECT i_node, array_agg(j_node) AS j_nodes_arr
    FROM stage_nw.successive_nodes
    GROUP BY i_node
    ORDER BY i_node
  LOOP
    n_routed := n_routed + 1;
    IF n_routed % 1000 = 0 THEN
      RAISE NOTICE '%/% one-to-many node pairs processed ...', n_routed, n_otm;
    END IF;
    INSERT INTO stage_nw.node_pair_routes (
      start_node, end_node, path_seq, node, edge
    )
    SELECT
      route_rec.i_node  AS start_node,
      end_vid           AS end_node,
      path_seq,
      node,
      edge
    FROM pgr_Dijkstra(
      esql,
      route_rec.i_node,
      route_rec.j_nodes_arr
    );
  END LOOP;

  RAISE NOTICE 'All node pairs processed.';

  UPDATE stage_nw.successive_nodes AS upd
  SET routed = true
  FROM (
    SELECT DISTINCT start_node, end_node
    FROM stage_nw.node_pair_routes
  ) AS npr
  WHERE upd.i_node = npr.start_node
    AND upd.j_node = npr.end_node;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'Routes found for %/% successive node pairs', cnt, n_total;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.route_node_pairs IS
'Find a route sequence along nw.links edges
for each stop node pair based on stage_gtfs.successive_stops,
nw.nodes and nw.stops, using pgr_Dijkstra.
Note that routes are not between stops but their respective NODES.
Results are stored in stage_nw.node_pair_routes.';
