/*
 * Create tables for the network model schema.
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\c sujuiko;

BEGIN;
\echo Creating nw schema ...

CREATE SCHEMA IF NOT EXISTS nw;

CREATE TABLE nw.links (
  linkid        integer           PRIMARY KEY,
  inode         integer,
  jnode         integer,
  mode          public.mode_type  NOT NULL,
  cost          double precision,
  rcost         double precision,
  osm_data      jsonb,
  geom          geometry(LINESTRING, 3067),
  wgs_geom      geometry(LINESTRING, 4326)
);
CREATE INDEX links_geom_idx
  ON nw.links
  USING GIST (geom);
CREATE INDEX links_wgs_geom_idx
  ON nw.links
  USING GIST (wgs_geom);
CREATE INDEX links_mode_idx
  ON nw.links (mode);

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

CREATE TABLE nw.stops (
  stopid      integer           PRIMARY KEY,
  nodeid      integer           NOT NULL,
  mode        public.mode_type  NOT NULL,
  code        text,
  name        text,
  descr       text,
  parent      integer
);
CREATE INDEX stops_nodeid_idx
  ON nw.stops (nodeid);
CREATE INDEX stops_mode_idx
  ON nw.stops (mode);
CREATE INDEX stops_code_idx
  ON nw.stops (code);

COMMIT;
