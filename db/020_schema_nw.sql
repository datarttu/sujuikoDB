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
  length_m      float8 GENERATED ALWAYS AS (ST_Length(geom)) STORED,
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

CREATE VIEW nw.view_link_directed AS (
  WITH oneways AS (
    SELECT
      link_id,
      false AS link_reversed,
      i_node,
      j_node,
      oneway,
      length_m,
      link_modes,
      link_label,
      data_source,
      source_date,
      geom
    FROM nw.link
    UNION
    SELECT
      link_id,
      true AS link_reversed,
      j_node AS i_node,
      i_node AS j_node,
      oneway,
      length_m,
      link_modes,
      link_label,
      data_source,
      source_date,
      ST_Reverse(geom) AS geom
    FROM nw.link
    WHERE NOT oneway
  )
  SELECT
    row_number() OVER (ORDER BY link_id, link_reversed) AS uniq_link_id,
    *
  FROM oneways
);

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

-- STOPS
CREATE TABLE nw.stop (
  stop_id             integer PRIMARY KEY,
  link_id             integer REFERENCES nw.link(link_id),
  link_reversed       boolean,
  location_on_link    float8,
  distance_from_link  float8,
  link_ref_manual     boolean DEFAULT false,
  stop_radius_m       float8,
  stop_mode           nw.vehicle_mode,
  stop_code           text,
  stop_name           text,
  stop_place          text,
  parent_stop_id      integer,
  source_date         date,
  errors              text[],
  geom                geometry(POINT, 3067)
);

CREATE INDEX ON nw.stop USING GIST(geom);

CREATE VIEW nw.view_stop_wkt AS (
  SELECT
    stop_id,
    stop_radius_m,
    stop_mode,
    stop_code,
    stop_name,
    stop_place,
    parent_stop_id,
    source_date,
    ST_AsText(geom) AS geom_text
  FROM nw.stop
);

CREATE FUNCTION nw.tg_insert_wkt_stop()
RETURNS trigger
AS $$
BEGIN
  INSERT INTO nw.stop (
    stop_id, stop_radius_m, stop_mode, stop_code, stop_name, stop_place,
    parent_stop_id, source_date, geom
    )
    VALUES (
      NEW.stop_id, NEW.stop_radius_m, NEW.stop_mode, NEW.stop_code,
      NEW.stop_name, NEW.stop_place, NEW.parent_stop_id, NEW.source_date,
      ST_GeomFromText(NEW.geom_text, 3067)
  );
  RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER tg_insert_wkt_stop
INSTEAD OF INSERT ON nw.view_stop_wkt
FOR EACH ROW EXECUTE PROCEDURE nw.tg_insert_wkt_stop();

-- ROUTE VERSIONS
CREATE TABLE nw.route_version (
  route_ver_id  text PRIMARY KEY,
  route         text NOT NULL,
  dir           smallint NOT NULL CHECK (dir IN (1, 2)),
  valid_during  daterange NOT NULL,
  route_mode    nw.vehicle_mode NOT NULL,
  errors        text[],

  EXCLUDE USING GIST (route_ver_id WITH =, valid_during WITH &&)
);

CREATE TABLE nw.stop_on_route (
  route_ver_id    text NOT NULL REFERENCES nw.route_version(route_ver_id),
  stop_seq        integer NOT NULL CHECK (stop_seq > 0),
  stop_id         integer NOT NULL REFERENCES nw.stop(stop_id),
  active_place    text,
  errors          text[],

  PRIMARY KEY (route_ver_id, stop_seq)
);

CREATE VIEW nw.view_stop_on_route_expanded AS (
  SELECT
    rv.route_ver_id,
    rv.route,
    rv.dir,
    lower(rv.valid_during) AS valid_from,
    upper(rv.valid_during) AS valid_until,
    rv.route_mode,
    sor.stop_seq,
    sor.active_place,
    st.stop_id,
    st.link_id,
    st.link_reversed,
    li.i_node,
    li.j_node,
    st.geom
  FROM nw.route_version AS rv
  INNER JOIN nw.stop_on_route AS sor
    ON rv.route_ver_id = sor.route_ver_id
  INNER JOIN nw.stop AS st
    ON sor.stop_id = st.stop_id
  LEFT JOIN nw.view_link_directed AS li
    ON (st.link_id = li.link_id AND st.link_reversed = li.link_reversed)
);

CREATE TABLE nw.manual_vianode_on_route (
  route_ver_id    text NOT NULL REFERENCES nw.route_version(route_ver_id),
  after_stop_seq  integer NOT NULL CHECK (after_stop_seq >= 0),
  sub_seq         integer NOT NULL CHECK (sub_seq > 0) DEFAULT 1,
  node_id         integer NOT NULL REFERENCES nw.node(node_id),
  errors          text[],

  PRIMARY KEY (route_ver_id, after_stop_seq, sub_seq)
);

CREATE VIEW nw.view_vianode_on_route AS (
  WITH significant_stops AS (
    SELECT
      route_ver_id,
      stop_seq,
      stop_id,
      CASE WHEN stop_seq = (min(stop_seq) OVER (PARTITION BY route_ver_id))
        THEN i_node ELSE j_node
      END AS node_id,
      lag(j_node) OVER (PARTITION BY route_ver_id ORDER BY stop_seq) AS previous_j_node
    FROM nw.view_stop_on_route_expanded
  ),
  union_stop_manual AS (
    SELECT
      route_ver_id,
      stop_seq,
      0::integer AS sub_seq,
      stop_id,
      node_id
    FROM significant_stops
    WHERE node_id <> previous_j_node
      OR previous_j_node IS NULL
    UNION ALL
    SELECT
      route_ver_id,
      after_stop_seq AS stop_seq,
      sub_seq,
      NULL::integer AS stop_id,
      node_id
    FROM nw.manual_vianode_on_route
  )
  SELECT
    usm.route_ver_id,
    row_number() OVER w_rtver AS node_seq,
    usm.stop_seq,
    usm.sub_seq,
    usm.stop_id,
    usm.node_id,
    nd.geom
  FROM union_stop_manual  AS usm
  LEFT JOIN nw.node       AS nd
    ON usm.node_id = nd.node_id
  WINDOW w_rtver AS (PARTITION BY usm.route_ver_id ORDER BY usm.stop_seq, usm.sub_seq)
  ORDER BY 1, 2
);

CREATE TABLE nw.link_on_route (
  route_ver_id  text NOT NULL REFERENCES nw.route_version(route_ver_id),
  link_seq      integer NOT NULL CHECK (link_seq > 0),
  link_id       integer NOT NULL REFERENCES nw.link(link_id),
  link_reversed boolean NOT NULL,
  errors        text[],

  PRIMARY KEY (route_ver_id, link_seq)
);

CREATE VIEW nw.view_link_on_route_geom AS (
  SELECT
    rv.route_ver_id,
    rv.route,
    rv.dir,
    lower(rv.valid_during)  AS valid_from,
    upper(rv.valid_during)  AS valid_until,
    rv.route_mode,
    lor.link_seq,
    ld.link_id,
    ld.length_m AS link_length_m,
    sum(ld.length_m) OVER rtver_grp AS cumul_length_m,
    ld.i_node,
    ld.j_node,
    ld.geom
  FROM nw.route_version             AS rv
  INNER JOIN nw.link_on_route       AS lor
    ON rv.route_ver_id = lor.route_ver_id
  INNER JOIN nw.view_link_directed  AS ld
    ON (lor.link_id = ld.link_id AND lor.link_reversed = ld.link_reversed)
  WINDOW rtver_grp AS (PARTITION BY rv.route_ver_id ORDER BY lor.link_seq)
);

-- SECTIONS FOR ANALYSIS
CREATE TABLE nw.section (
  section_id        text PRIMARY KEY,
  description       text,
  report            boolean DEFAULT true,
  rotation          float8 DEFAULT 0.0,
  via_nodes         integer[],
  errors            text[]
);

CREATE TABLE nw.link_on_section (
  section_id        text NOT NULL REFERENCES nw.section(section_id),
  link_seq          integer NOT NULL CHECK (link_seq > 0),
  link_id           integer NOT NULL REFERENCES nw.link(link_id),
  link_reversed     boolean NOT NULL,
  errors            text[],

  PRIMARY KEY (section_id, link_seq)
);

CREATE VIEW nw.view_link_on_section_geom AS (
  SELECT
    sec.section_id,
    sec.description,
    sec.report,
    sec.rotation,
    sec.via_nodes,
    los.link_seq,
    ld.link_id,
    ld.link_reversed,
    ld.length_m AS link_length_m,
    sum(ld.length_m) OVER (PARTITION BY sec.section_id ORDER BY los.link_seq) AS cumul_length_m,
    ld.link_label,
    ld.i_node,
    ld.j_node,
    ld.geom
  FROM nw.section                   AS sec
  INNER JOIN nw.link_on_section     AS los
    ON sec.section_id = los.section_id
  INNER JOIN nw.view_link_directed  AS ld
    ON (los.link_id = ld.link_id AND los.link_reversed = ld.link_reversed)
);
