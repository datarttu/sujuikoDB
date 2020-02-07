/*
Create tables for the network model schema.

Arttu K 2020-02
*/
\c sujuiko;

CREATE SCHEMA IF NOT EXISTS nw;

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

/*
Even though OSM ways can be oneway or two-way,
we handle two-way streets as separate links here.
Thus the "reverse cost" options will be never used with pgrouting.
The two-way links should have the same geometry
except that the points are listed in reverse order.
*/
CREATE TABLE nw.links (
  inode        integer REFERENCES nw.nodes (nodeid),
  jnode        integer REFERENCES nw.nodes (nodeid),
  modes        public.mode_type[] NOT NULL,
  osm_tags     jsonb,
  PRIMARY KEY (inode, jnode),
  CONSTRAINT nodes CHECK (inode <> jnode)
);
SELECT AddGeometryColumn('nw', 'links', 'geom', 3067, 'LINESTRING', 2);
CREATE INDEX links_geom_idx
  ON nw.links
  USING GIST (geom);
CREATE INDEX links_modes_idx
  ON nw.links
  USING GIN (modes);

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
CREATE INDEX stops_modes_idx
  ON nw.stops
  USING GIN (modes);
