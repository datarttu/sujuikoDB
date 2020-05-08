CREATE OR REPLACE FUNCTION nw.create_node_table()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
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
  ALTER TABLE nw.links_vertices_pgr
  RENAME TO nodes;
  ALTER TABLE nw.nodes
  RENAME COLUMN id TO nodeid;
  ALTER TABLE nw.nodes
  RENAME COLUMN the_geom TO geom;
  ALTER TABLE nw.nodes
  ADD COLUMN wgs_geom geometry(POINT, 4326);
  UPDATE nw.nodes
  SET wgs_geom = ST_Transform(geom, 4326);
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% nodes created for nw.links', cnt;
  RETURN 'OK';

END;
$$;
COMMENT ON FUNCTION nw.create_node_table IS
'Create nw.nodes table by building topology from nw.links
and renaming table and columns created by pgr_createTopology.';
