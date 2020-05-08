CREATE TABLE stage_nw.successive_nodes (
  i_node      integer     NOT NULL,
  j_node      integer     NOT NULL,
  routed      boolean     DEFAULT false,
  PRIMARY KEY (i_node, j_node)
);

CREATE OR REPLACE FUNCTION stage_nw.populate_successive_nodes()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM stage_nw.successive_nodes;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_nw.successive_nodes', cnt;

  INSERT INTO stage_nw.successive_nodes (
    i_node, j_node
  )
  SELECT DISTINCT
    b.nodeid    AS i_node,
    c.nodeid    AS j_node
  FROM stage_gtfs.successive_stops  AS a
  INNER JOIN nw.stops               AS b
    ON a.i_stop = b.stopid
  INNER JOIN nw.stops               AS c
    ON a.j_stop = c.stopid;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% node pairs inserted into stage_nw.successive_nodes', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.populate_successive_nodes IS
'Stop node pairs based on stage_gtfs.successive_stops,
for finding network routes between stops.
Only nodes found in nw.stops are included.
Moreover, one node pair may represent multiple stop pairs
when stops share the same node.';
