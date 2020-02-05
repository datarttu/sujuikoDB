/*
Create tables for the network model schema.

Arttu K 2020-02
*/
\c sujuiko;

/*
TODO:

Can we pre-define edge and node tables for pgrouting,
or must they be created dynamically?
*/

CREATE TABLE nw.nodes (
  nodeid       integer PRIMARY KEY,
  osm_tags     jsonb
);
SELECT AddGeometryColumn('nw', 'nodes', 'geom', 3067, 'POINT', 2);
CREATE INDEX nodes_geom_idx
  ON nw.nodes
  USING GIST (geom);

CREATE TABLE nw.links (
  inode        integer REFERENCES nw.nodes (nodeid),
  jnode        integer REFERENCES nw.nodes (nodeid),
  oneway       boolean NOT NULL,
  modes        public.mode_type[] NOT NULL,
  osm_tags     jsonb,
  PRIMARY KEY (inode, jnode),
  CONSTRAINT nodes CHECK (inode <> jnode)
);
SELECT AddGeometryColumn('nw', 'links', 'geom', 3067, 'LINESTRING', 2);
CREATE INDEX links_geom_idx
  ON nw.links
  USING GIST (geom);

CREATE TABLE nw.stops (
  stopid       integer PRIMARY KEY,
  nodeid       integer REFERENCES nw.nodes (nodeid),
  modes        public.mode_type[] NOT NULL,
  code         text,
  name         text,
  descr        text,
  parent       integer
);
CREATE INDEX stops_nodeid_idx
  ON nw.stops (nodeid);
