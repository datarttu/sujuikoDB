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