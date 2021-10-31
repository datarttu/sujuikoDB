CREATE SCHEMA nw;

COMMENT ON SCHEMA nw IS
'Transit network, route versions and analysis sections on it.';

CREATE TYPE nw.vehicle_mode AS enum('bus', 'tram');

-- NODES
CREATE TABLE nw.node (
  node_id       serial PRIMARY KEY,
  errors        text[],
  geom          geometry(POINT, 3067) NOT NULL
);

CREATE INDEX ON nw.node USING GIST(geom);

COMMENT ON TABLE nw.node IS
'Geographical points where links start and end.';
COMMENT ON COLUMN nw.node.node_id IS
'Unique node identifier.';
COMMENT ON COLUMN nw.node.errors IS
'Error codes produced by validations.';
COMMENT ON COLUMN nw.node.geom IS
'Node POINT geometry in ETRS-TM35 coordinates.';

-- LINKS
CREATE TABLE nw.link (
  link_id       integer PRIMARY KEY CHECK (link_id > 0),
  i_node        integer DEFAULT 0 REFERENCES nw.node(node_id),
  j_node        integer DEFAULT 0 REFERENCES nw.node(node_id),
  oneway        boolean,
  length_m      float8 GENERATED ALWAYS AS (ST_Length(geom)) STORED,
  link_modes    nw.vehicle_mode[] DEFAULT ARRAY['bus'::nw.vehicle_mode],
  link_label    text,
  data_source   text DEFAULT 'Manual',
  source_date   date DEFAULT CURRENT_DATE,
  errors        text[],
  geom          geometry(LINESTRING, 3067)
);

CREATE INDEX ON nw.link USING BTREE(i_node);
CREATE INDEX ON nw.link USING BTREE(j_node);
CREATE INDEX ON nw.link USING GIST(geom);

COMMENT ON TABLE nw.link IS
'Street or tram network parts where buses and/or trams can run.';
COMMENT ON COLUMN nw.link.link_id IS
'Unique link identifier. May be inherited from data source such as Digiroad.';
COMMENT ON COLUMN nw.link.i_node IS
'Node id at the start of the link geometry.';
COMMENT ON COLUMN nw.link.j_node IS
'Node id at the end of the link geometry.';
COMMENT ON COLUMN nw.link.oneway IS
'true = can be traversed to the geometry direction only, false = can be traversed both ways.';
COMMENT ON COLUMN nw.link.length_m IS
'Link length in meters, generated from the geometry.';
COMMENT ON COLUMN nw.link.link_modes IS
'Allowed vehicle modes on the link.';
COMMENT ON COLUMN nw.link.link_label IS
'Name or other readable identifier for the link (e.g. street name).';
COMMENT ON COLUMN nw.link.data_source IS
'Original data source name, e.g. `Digiroad`, or `Manual` for hand-edited links.';
COMMENT ON COLUMN nw.link.source_date IS
'Import or modification date of the link.';
COMMENT ON COLUMN nw.link.errors IS
'Error codes produced by validations.';
COMMENT ON COLUMN nw.link.geom IS
'Link LINESTRING geometry in ETRS-TM35 coordinates.';

CREATE FUNCTION nw.tg_set_link_node_references()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  existing_node_id integer;
BEGIN
  -- Link start, i_node
  SELECT INTO existing_node_id nd.node_id
  FROM nw.node AS nd
  WHERE ST_StartPoint(NEW.geom) && nd.geom
  LIMIT 1;
  IF NOT FOUND THEN
    WITH inserted_node AS (
      INSERT INTO nw.node(geom)
      VALUES (ST_StartPoint(NEW.geom))
      RETURNING node_id
    )
    SELECT INTO existing_node_id node_id
    FROM inserted_node;
  END IF;
  NEW.i_node := existing_node_id;

  existing_node_id := NULL;

  -- Link end, j_node
  SELECT INTO existing_node_id nd.node_id
  FROM nw.node AS nd
  WHERE ST_EndPoint(NEW.geom) && nd.geom
  LIMIT 1;
  IF NOT FOUND THEN
    WITH inserted_node AS (
      INSERT INTO nw.node(geom)
      VALUES (ST_EndPoint(NEW.geom))
      RETURNING node_id
    )
    SELECT INTO existing_node_id node_id
    FROM inserted_node;
  END IF;
  NEW.j_node := existing_node_id;

  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.tg_set_link_node_references IS
'When creating or updating a link geometry, set is i_node and j_node references
to nodes located at the corresponding ends of the geometry.
If no node exists at an end, create a new node there.';

CREATE TRIGGER tg_set_link_node_references
BEFORE INSERT OR UPDATE OF geom ON nw.link
FOR EACH ROW EXECUTE PROCEDURE nw.tg_set_link_node_references();

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

COMMENT ON VIEW nw.view_link_directed IS
'Two-way links duplicated into directed oneway versions, and oneway links as usual.';

CREATE VIEW nw.view_link_wkt AS (
  SELECT
    link_id,
    oneway,
    link_modes,
    link_label,
    data_source,
    source_date,
    ST_AsText(geom) AS geom_text
  FROM nw.link
);

COMMENT ON VIEW nw.view_link_wkt IS
'Link geometries as well-known text. Allows copying link data from csv files with WKT geometries.';

CREATE FUNCTION nw.tg_insert_wkt_link()
RETURNS trigger
AS $$
BEGIN
  INSERT INTO nw.link (
    link_id, oneway, link_modes,
    link_label, data_source, source_date,
    geom
    )
    VALUES (
      NEW.link_id, NEW.oneway, NEW.link_modes,
      NEW.link_label, NEW.data_source, NEW.source_date,
      ST_GeomFromText(NEW.geom_text, 3067)
  );
  RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION nw.tg_insert_wkt_link IS
'Stores WKT geoms inserted into nw.view_link_wkt view as binary geoms in the actual table.';

CREATE TRIGGER tg_insert_wkt_link
INSTEAD OF INSERT ON nw.view_link_wkt
FOR EACH ROW EXECUTE PROCEDURE nw.tg_insert_wkt_link();

-- STOPS
CREATE TABLE nw.stop (
  stop_id             integer PRIMARY KEY,
  link_id             integer REFERENCES nw.link(link_id) ON DELETE SET NULL,
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

COMMENT ON TABLE nw.stop IS
'Transit stop points where vehicles can have stopped for boarding and alighting passengers.';
COMMENT ON COLUMN nw.stop.stop_id IS
'Unique stop identifier. Equals GTFS and Jore stop id.';
COMMENT ON COLUMN nw.stop.link_id IS
'The link that the stop is located along. See nw.get_stop_link_refs() function.';
COMMENT ON COLUMN nw.stop.link_reversed IS
'true = link_id refers to the reversed version of a two-way link.';
COMMENT ON COLUMN nw.stop.location_on_link IS
'Projected relative stop location along the link distance, between 0.0 and 1.0. Use link length with this to get the absolute location.';
COMMENT ON COLUMN nw.stop.distance_from_link IS
'Distance between original stop geometry and projected geometry along the link.';
COMMENT ON COLUMN nw.stop.link_ref_manual IS
'true = link ref values are protected from automatic updates (e.g. if they have been modified by hand).';
COMMENT ON COLUMN nw.stop.stop_radius_m IS
'Distance that th stop "covers" along the link before and after the projected point.';
COMMENT ON COLUMN nw.stop.stop_mode IS
'Allowed vehicle mode of the stop. (In Jore, a physical multi-mode stop is split into one stop entry per mode.)';
COMMENT ON COLUMN nw.stop.stop_code IS
'Short identifier of the stop, e.g. H2034. Can group together per-mode entries of a multi-mode stop.';
COMMENT ON COLUMN nw.stop.stop_name IS
'Human-readable nme of the stop.';
COMMENT ON COLUMN nw.stop.stop_place IS
'Hastus place code of the stop, used in schedule planning. Can group together multiple stops.';
COMMENT ON COLUMN nw.stop.parent_stop_id IS
'Groups together multiple stops of a terminal or other area.';
COMMENT ON COLUMN nw.stop.source_date IS
'Import or modification date of the stop.';
COMMENT ON COLUMN nw.stop.errors IS
'Error codes produced by stop validations.';
COMMENT ON COLUMN nw.stop.geom IS
'Stop POINT geometry in ETRS-TM35 coordinates.';

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

COMMENT ON VIEW nw.view_stop_wkt IS
'Stop geometries as well-known text. Allows copying stop data from csv files with WKT geometries.';

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

COMMENT ON FUNCTION nw.tg_insert_wkt_stop IS
'Stores WKT geoms inserted into nw.view_stop_wkt view as binary geoms in the actual table.';

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

COMMENT ON TABLE nw.route_version IS
'Transit route & direction patterns that have been valid between given dates.';
COMMENT ON COLUMN nw.route_version.route_ver_id IS
'Unique route version identifier. Should be of form <route>_<dir>_<startdate>_<enddate>.';
COMMENT ON COLUMN nw.route_version.route IS
'Route identifier.';
COMMENT ON COLUMN nw.route_version.dir IS
'Direction identifier (1 or 2).';
COMMENT ON COLUMN nw.route_version.valid_during IS
'Date range during which the route version has been valid.';
COMMENT ON COLUMN nw.route_version.route_mode IS
'Vehicle mode of the route version.';
COMMENT ON COLUMN nw.route_version.errors IS
'Error codes produced by validations.';

CREATE TABLE nw.stop_on_route (
  route_ver_id    text NOT NULL REFERENCES nw.route_version(route_ver_id),
  stop_seq        integer NOT NULL CHECK (stop_seq > 0),
  stop_id         integer NOT NULL REFERENCES nw.stop(stop_id),
  active_place    text,
  errors          text[],

  PRIMARY KEY (route_ver_id, stop_seq)
);

COMMENT ON TABLE nw.stop_on_route IS
'Ordered transit stops used by route versions along their routes.';
COMMENT ON COLUMN nw.stop_on_route.route_ver_id IS
'Route version identifier.';
COMMENT ON COLUMN nw.stop_on_route.stop_seq IS
'Order number of the stop within the route version, starting from 1.';
COMMENT ON COLUMN nw.stop_on_route.stop_id IS
'Stop identifier.';
COMMENT ON COLUMN nw.stop_on_route.active_place IS
'Possible Hastus place id used in schedule planning of the route version. Should match `stop_place` of the stop.';
COMMENT ON COLUMN nw.stop_on_route.errors IS
'Error codes produced by validations.';

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

COMMENT ON VIEW nw.view_stop_on_route_expanded IS
'Stops on routes with related attributes, such as stop geometry, mainly for visualization.';

CREATE TABLE nw.manual_vianode_on_route (
  route_ver_id    text NOT NULL REFERENCES nw.route_version(route_ver_id),
  after_stop_seq  integer NOT NULL CHECK (after_stop_seq >= 0),
  sub_seq         integer NOT NULL CHECK (sub_seq > 0) DEFAULT 1,
  node_id         integer NOT NULL REFERENCES nw.node(node_id),
  errors          text[],

  PRIMARY KEY (route_ver_id, after_stop_seq, sub_seq)
);

COMMENT ON TABLE nw.manual_vianode_on_route IS
'Additional nodes used when creating link_on_route paths along the network.';
COMMENT ON COLUMN nw.manual_vianode_on_route.route_ver_id IS
'Route version identifier.';
COMMENT ON COLUMN nw.manual_vianode_on_route.after_stop_seq IS
'Stop order number after which this node is used as routing via-node.';
COMMENT ON COLUMN nw.manual_vianode_on_route.sub_seq IS
'Order number of via-node, in case there are multiple ones between successive stops on route.';
COMMENT ON COLUMN nw.manual_vianode_on_route.node_id IS
'Node identifier.';
COMMENT ON COLUMN nw.manual_vianode_on_route.errors IS
'Error codes produced by validations.';

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

COMMENT ON VIEW nw.view_vianode_on_route IS
'All via-nodes for link_on_route paths of route versions: stops and manual via-nodes in correct order.';

CREATE TABLE nw.link_on_route (
  route_ver_id  text NOT NULL REFERENCES nw.route_version(route_ver_id),
  link_seq      integer NOT NULL CHECK (link_seq > 0),
  link_id       integer NOT NULL REFERENCES nw.link(link_id),
  link_reversed boolean NOT NULL,
  errors        text[],

  PRIMARY KEY (route_ver_id, link_seq)
);

COMMENT ON TABLE nw.link_on_route IS
'Ordered links (directed) that form a continuous path for a route version. Correct link matches are found from `nw.view_link_directed` by `link_id` and `link_reversed`.';
COMMENT ON COLUMN nw.link_on_route.route_ver_id IS
'Route version identifier.';
COMMENT ON COLUMN nw.link_on_route.route_ver_id IS
'Order number of the link within the route version.';
COMMENT ON COLUMN nw.link_on_route.route_ver_id IS
'Link identifier.';
COMMENT ON COLUMN nw.link_on_route.route_ver_id IS
'true = refers to the reversed version of the link.';
COMMENT ON COLUMN nw.link_on_route.route_ver_id IS
'Error codes produced by validations.';

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

COMMENT ON VIEW nw.view_link_on_route_geom IS
'Links on routes with related attributes, such as link geometries, mainly for visualization.';

-- SECTIONS FOR ANALYSIS
CREATE TABLE nw.section (
  section_id        text PRIMARY KEY,
  description       text,
  section_group     text,
  section_order     integer,
  report            boolean DEFAULT true,
  rotation          float8 DEFAULT 0.0,
  via_nodes         integer[],
  errors            text[]
);

COMMENT ON TABLE nw.section IS
'Sections of network for analysis (continuous paths of links).';
COMMENT ON COLUMN nw.section.section_id IS
'Unique section identifier.';
COMMENT ON COLUMN nw.section.description IS
'Description of the section.';
COMMENT ON COLUMN nw.section.section_group IS
'Group identifier of multiple sections.';
COMMENT ON COLUMN nw.section.section_order IS
'Order number of a section within `section_group`.';
COMMENT ON COLUMN nw.section.report IS
'true = include section in reporting, false = omit from report / store as "passive" section.';
COMMENT ON COLUMN nw.section.rotation IS
'Section rotation in degrees, for map visualization.';
COMMENT ON COLUMN nw.section.via_nodes IS
'Ordered via-nodes for creating the section path.';
COMMENT ON COLUMN nw.section.errors IS
'Error codes produced by validations.';

CREATE TABLE nw.link_on_section (
  section_id        text NOT NULL REFERENCES nw.section(section_id),
  link_seq          integer NOT NULL CHECK (link_seq > 0),
  link_id           integer NOT NULL REFERENCES nw.link(link_id),
  link_reversed     boolean NOT NULL,
  errors            text[],

  PRIMARY KEY (section_id, link_seq)
);

COMMENT ON TABLE nw.link_on_section IS
'Ordered links within sections, forming continuous section paths on the network.';
COMMENT ON COLUMN nw.link_on_section.section_id IS
'Section identifier.';
COMMENT ON COLUMN nw.link_on_section.link_seq IS
'Order number of the link within the section.';
COMMENT ON COLUMN nw.link_on_section.link_id IS
'Link identifier.';
COMMENT ON COLUMN nw.link_on_section.link_reversed IS
'true = refers to the reversed version of the link.';
COMMENT ON COLUMN nw.link_on_section.errors IS
'Error codes produced by validations.';

CREATE VIEW nw.view_link_on_section_geom AS (
  SELECT
    sec.section_id,
    sec.description,
    sec.section_group,
    sec.section_order,
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

COMMENT ON VIEW nw.view_link_on_section_geom IS
'Section links with related attributes, such as link geometries, mainly for visualization.';

CREATE VIEW nw.view_section_ij_line AS (
  SELECT
    se.section_id,
    se.description,
    se.section_group,
    se.section_order,
    se.report,
    se.rotation,
    se.via_nodes[1]                         AS i_node,
    se.via_nodes[cardinality(se.via_nodes)] AS j_node,
    ST_MakeLine(ind.geom, jnd.geom)         AS geom
  FROM nw.section AS se
  INNER JOIN nw.node  AS ind
    ON (se.via_nodes[1] = ind.node_id)
  INNER JOIN nw.node  AS jnd
    ON (se.via_nodes[cardinality(se.via_nodes)] = jnd.node_id)
  WHERE cardinality(se.via_nodes) > 1
);

COMMENT ON VIEW nw.view_section_ij_line IS
'Sections as LINESTRING geoms from start to end vianode. Used for simple visualization and editing of sections.';

CREATE FUNCTION nw.tg_upsert_section_ij_line()
RETURNS trigger
LANGUAGE PLPGSQL
AS $function$
DECLARE
  closest_i_node_id integer;
  closest_j_node_id integer;
BEGIN

  SELECT node_id INTO closest_i_node_id
  FROM (
    SELECT
      nd.node_id,
      ST_Distance(nd.geom, ST_StartPoint(NEW.geom))
    FROM nw.node  AS nd
    WHERE ST_DWithin(nd.geom, ST_StartPoint(NEW.geom), 100.0)
    ORDER BY 2
    LIMIT 1
  ) AS _;

  SELECT node_id INTO closest_j_node_id
  FROM (
    SELECT
      nd.node_id,
      ST_Distance(nd.geom, ST_EndPoint(NEW.geom))
    FROM nw.node  AS nd
    WHERE ST_DWithin(nd.geom, ST_EndPoint(NEW.geom), 100.0)
    ORDER BY 2
    LIMIT 1
  ) AS _;

  IF TG_OP = 'INSERT' THEN
    INSERT INTO nw.section(
      section_id, description, section_group, report, rotation, via_nodes
    ) VALUES (
      NEW.section_id,
      NEW.description,
      NEW.section_group,
      NEW.section_order,
      NEW.report,
      NEW.rotation,
      ARRAY[closest_i_node_id, closest_j_node_id]
    );
  END IF;

  IF TG_OP = 'UPDATE' THEN
    UPDATE nw.section
    SET
      description = NEW.description,
      section_group = NEW.section_group,
      section_order = NEW.section_order,
      report = NEW.report,
      rotation = NEW.rotation,
      via_nodes = ARRAY[closest_i_node_id, closest_j_node_id]
    WHERE section_id = NEW.section_id;
  END IF;

  RETURN NEW;
END;
$function$;

COMMENT ON FUNCTION nw.tg_upsert_section_ij_line IS
'Enables editing section start and end nodes visually by getting the node references from the nodes closest to the end points of the input geometry.';

CREATE TRIGGER tg_upsert_section_ij_line
INSTEAD OF INSERT OR UPDATE ON nw.view_section_ij_line
FOR EACH ROW EXECUTE PROCEDURE nw.tg_upsert_section_ij_line();

CREATE FUNCTION nw.tg_delete_section_ij_line()
RETURNS trigger
LANGUAGE PLPGSQL
AS $function$
BEGIN

  DELETE FROM nw.link_on_section
  WHERE section_id = OLD.section_id;

  DELETE FROM nw.section
  WHERE section_id = OLD.section_id;

  RETURN OLD;
END;
$function$;

COMMENT ON FUNCTION nw.tg_delete_section_ij_line IS
'Handles deleting a section and its links when a DELETE statement of the corresponding section_ij_line entry is issued.';

CREATE TRIGGER tg_delete_section_ij_line
INSTEAD OF DELETE ON nw.view_section_ij_line
FOR EACH ROW EXECUTE PROCEDURE nw.tg_delete_section_ij_line();

CREATE VIEW nw.view_section_geom AS (
  SELECT
    sec.section_id,
    sec.description,
    sec.section_group,
    sec.section_order,
    sec.report,
    sec.rotation,
    sec.via_nodes,
    sg.geom
  FROM nw.section                   AS sec
  INNER JOIN (
    SELECT
      los.section_id,
      ST_MakeLine(ld.geom ORDER BY los.link_seq) AS geom
      FROM nw.link_on_section     AS los
      INNER JOIN nw.view_link_directed  AS ld
        ON (los.link_id = ld.link_id AND los.link_reversed = ld.link_reversed)
      GROUP BY los.section_id
  ) AS sg
    ON sec.section_id = sg.section_id
);

COMMENT ON VIEW nw.view_section_geom IS
'Section paths as LINESTRING geoms combined from links on section.';

CREATE VIEW nw.view_section_stop_points AS (
  WITH cumul_links AS (
    SELECT
      los.section_id,
      los.link_seq,
      los.link_id,
      los.link_reversed,
      ld.length_m AS link_length_m,
      sum(ld.length_m) OVER w_section - ld.length_m AS link_i_dist,
      ld.geom AS link_geom
    FROM nw.link_on_section AS los
    INNER JOIN nw.view_link_directed AS ld
      ON (los.link_id = ld.link_id AND los.link_reversed = ld.link_reversed)
    WINDOW w_section AS (PARTITION BY los.section_id ORDER BY los.link_seq)
  )
  SELECT
    cl.section_id,
    cl.link_seq,
    st.stop_id,
    st.stop_code,
    st.stop_radius_m,
    cl.link_i_dist + (cl.link_length_m * st.location_on_link) AS stop_cumul_loc_m,
    ST_LineInterpolatePoint(cl.link_geom, st.location_on_link) AS geom
  FROM cumul_links AS cl
  INNER JOIN nw.stop AS st
    ON (cl.link_id = st.link_id AND cl.link_reversed = st.link_reversed)
);

COMMENT ON VIEW nw.view_section_stop_points IS
'Stop points with attributes, such as geometry and cumulative location value, along sections. Mainly for visualization.';
