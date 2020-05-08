CREATE OR REPLACE FUNCTION nw.create_node_table()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN

  DELETE FROM nw.nodes;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'nw.nodes empty: % nodes deleted', cnt;

  PERFORM pgr_createTopology(
    'nw.links',
    0.01,
    the_geom    := 'geom',
    id          := 'linkid',
    source      := 'inode',
    target      := 'jnode',
    rows_where  := 'true',
    clean       := true
  );

  INSERT INTO nw.nodes (
    nodeid, cnt, chk, ein, eout, geom
  )
  SELECT id, cnt, chk, ein, eout, the_geom
  FROM nw.links_vertices_pgr
  ORDER BY nodeid;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% node rows inserted into nw.nodes', cnt;
  RETURN 'OK';

  DROP TABLE nw.links_vertices_pgr;

  UPDATE nw.nodes
  SET wgs_geom = ST_Transform(geom, 4326);
  RAISE NOTICE 'nw.nodes: wgs_geom set';

END;
$$;
COMMENT ON FUNCTION nw.create_node_table IS
'Run pgr_createTopology on nw.links edge table,
import result into nw.nodes table.';
