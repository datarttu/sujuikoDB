CREATE SCHEMA nw;

CREATE TYPE nw.vehicle_mode AS enum('bus', 'tram');

-- NODES
CREATE TABLE nw.node (
  node_id       integer PRIMARY KEY,
  errors        text[],
  geom          geometry(POINT, 3067) NOT NULL
  );

CREATE INDEX ON nw.node USING GIST(geom);

CREATE VIEW nw.view_node_wkt AS (
  SELECT
    node_id,
    ST_AsText(geom) AS geom_text
  FROM nw.node
  );

CREATE FUNCTION nw.tg_insert_wkt_node()
RETURNS trigger
AS $$
BEGIN
  INSERT INTO nw.node (node_id, geom)
  VALUES (
    NEW.node_id,
    ST_GeomFromText(NEW.geom_text, 3067)
  );
  RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER tg_insert_wkt_node
INSTEAD OF INSERT ON nw.view_node_wkt
FOR EACH ROW EXECUTE PROCEDURE nw.tg_insert_wkt_node();

-- LINKS
CREATE TABLE nw.link (
  link_id       integer PRIMARY KEY CHECK (link_id > 0),
  i_node        integer,
  j_node        integer,
  oneway        boolean,
  length_m      numeric GENERATED ALWAYS AS (ST_Length(geom)) STORED,
  link_modes    nw.vehicle_mode[],
  link_label    text,
  data_source   text,
  source_date   date,
  errors        text[],
  geom          geometry(LINESTRING, 3067)
  );

CREATE INDEX ON nw.link USING BTREE(i_node);
CREATE INDEX ON nw.link USING BTREE(j_node);
CREATE INDEX ON nw.link USING GIST(geom);

CREATE VIEW nw.view_link_wkt AS (
  SELECT
    link_id,
    i_node,
    j_node,
    oneway,
    link_modes,
    link_label,
    data_source,
    source_date,
    ST_AsText(geom) AS geom_text
  FROM nw.link
  );

CREATE FUNCTION nw.tg_insert_wkt_link()
RETURNS trigger
AS $$
BEGIN
  INSERT INTO nw.link (
    link_id, i_node, j_node, oneway, link_modes,
    link_label, data_source, source_date,
    geom
    )
    VALUES (
      NEW.link_id, NEW.i_node, NEW.j_node, NEW.oneway, NEW.link_modes,
      NEW.link_label, NEW.data_source, NEW.source_date,
      ST_GeomFromText(NEW.geom_text, 3067)
  );
  RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER tg_insert_wkt_link
INSTEAD OF INSERT ON nw.view_link_wkt
FOR EACH ROW EXECUTE PROCEDURE nw.tg_insert_wkt_link();

-- STOP VERSIONS
CREATE TABLE nw.stop_version (
  stop_id           integer NOT NULL,
  version_id        integer NOT NULL CHECK (version_id > 0),
  valid_range       daterange NOT NULL,
  link_id           integer NOT NULL REFERENCES nw.link(link_id),
  link_rel_covered  numrange,
  radius_m          numeric,
  stop_mode         nw.vehicle_mode,
  stop_code         text,
  stop_name         text,
  stop_place        text,
  parent_stop_id    integer,
  errors            text[],

  PRIMARY KEY (stop_id, version_id),
  EXCLUDE USING GIST (valid_range WITH &&)
  );

-- ROUTE VERSIONS
CREATE TABLE nw.route_version (
  route_ver_id  text PRIMARY KEY,
  route         text NOT NULL,
  dir           smallint NOT NULL CHECK (dir IN (1, 2)),
  version_id    integer NOT NULL,
  valid_range   daterange NOT NULL,
  route_mode    nw.vehicle_mode NOT NULL,
  errors        text[],

  EXCLUDE USING GIST (valid_range WITH &&)
  );

CREATE TABLE nw.route_stop (
  route_ver_id    text NOT NULL REFERENCES nw.route_version(route_ver_id),
  stop_seq        integer NOT NULL CHECK (stop_seq > 0),
  stop_id         integer NOT NULL,
  stop_version_id integer NOT NULL,
  active_place    text,

  PRIMARY KEY (route_ver_id, stop_seq),
  FOREIGN KEY (stop_id, stop_version_id) REFERENCES nw.stop_version (stop_id, version_id)
  );

CREATE TABLE nw.route_link (
  route_ver_id  text NOT NULL REFERENCES nw.route_version(route_ver_id),
  link_seq      integer NOT NULL CHECK (link_seq > 0),
  link_id       integer NOT NULL REFERENCES nw.link(link_id),
  link_dir      smallint NOT NULL CHECK (link_dir IN (-1, 1)),

  PRIMARY KEY (route_ver_id, link_seq)
  );

-- ANALYSIS SEGMENTS
CREATE TABLE nw.analysis_segment (
  analysis_seg_id   text PRIMARY KEY,
  description       text,
  report            boolean DEFAULT true,
  rotation          numeric DEFAULT 0.0,
  errors            text[]
  );

CREATE TABLE nw.analysis_seg_link (
  analysis_seg_id   text NOT NULL REFERENCES nw.analysis_segment(analysis_seg_id),
  link_seq          integer NOT NULL CHECK (link_seq > 0),
  link_id           integer NOT NULL REFERENCES nw.link(link_id),
  link_dir          smallint NOT NULL CHECK (link_dir IN (-1, 1)),

  PRIMARY KEY (analysis_seg_id, link_seq)
  );
