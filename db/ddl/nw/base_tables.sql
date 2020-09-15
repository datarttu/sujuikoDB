DROP SCHEMA IF EXISTS nw CASCADE;
CREATE SCHEMA nw;

CREATE TABLE nw.nodes (
  nodeid        serial                  PRIMARY KEY,
  geom          geometry(POINT, 3067)
);
COMMENT ON TABLE nw.nodes IS
'Represent separation points on street and track networks:
intersections, link ends, and locations where two links
with different attribute values must be separated.';

CREATE INDEX ON nw.nodes USING GIST (geom);

-- HSL area validation trigger
CREATE FUNCTION nw.validate_node_is_within_hsl_area()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  hsl_bbox  geometry(POLYGON, 3067);
BEGIN
  hsl_bbox := ST_SetSRID(
    'POLYGON((244592 6628611,255619 6788176,499650 6779684,499634 6619861,244592 6628611))'::geometry,
    3067
  );
  IF NOT NEW.geom && hsl_bbox THEN
    RAISE WARNING 'node % is outside HSL area, discarded', NEW.nodeid;
    RETURN NULL;
  ELSE RETURN NEW;
  END IF;
END;
$$;
COMMENT ON FUNCTION nw.validate_node_is_within_hsl_area() IS
'Ensures the `geom` of a new node is inside a bounding box containing the HSL area.';

CREATE TRIGGER validate_node_is_within_hsl_area
BEFORE INSERT OR UPDATE ON nw.nodes
FOR EACH ROW EXECUTE PROCEDURE nw.validate_node_is_within_hsl_area();

-- Duplicate location prevention trigger
CREATE FUNCTION nw.validate_node_unique_location()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  snap_tolerance  double precision;
  existing_node   integer;
BEGIN
  snap_tolerance := 1.0;
  SELECT INTO existing_node nd.nodeid FROM nw.nodes AS nd
  WHERE ST_DWithin(NEW.geom, nd.geom, snap_tolerance)
  LIMIT 1;
  IF existing_node IS NOT NULL THEN
    RAISE WARNING 'node % is too close to existing node %, discarded', NEW.nodeid, existing_node;
    RETURN NULL;
  ELSE RETURN NEW;
  END IF;
END;
$$;
COMMENT ON FUNCTION nw.validate_node_unique_location() IS
'Ensures the `geom` of a new node is no closer than 1 meters to an existing node.';

CREATE TRIGGER validate_node_unique_location
BEFORE INSERT OR UPDATE ON nw.nodes
FOR EACH ROW EXECUTE PROCEDURE nw.validate_node_unique_location();



CREATE TABLE nw.links (
  linkid        serial              PRIMARY KEY,
  inode         integer             NOT NULL REFERENCES nw.nodes(nodeid),
  jnode         integer             NOT NULL REFERENCES nw.nodes(nodeid),
  mode          text                NOT NULL CHECK (mode IN ('bus', 'tram')),
  oneway        boolean             NOT NULL,
  cost          double precision    GENERATED ALWAYS AS (ST_Length(geom)) STORED,
  rcost         double precision    GENERATED ALWAYS AS (CASE WHEN oneway THEN -1 ELSE ST_Length(geom) END) STORED,
  attributes    jsonb,
  geom          geometry(LINESTRING, 3067)  NOT NULL
);
COMMENT ON TABLE nw.links IS
'Represent connections between nodes.';

CREATE INDEX ON nw.links (mode);
CREATE INDEX ON nw.links USING GIST (geom);

-- inode and jnode location checks
-- These apply only if you try to modify inode / jnode directly.
-- When inserting a new link without existing nodes, they will be created.
CREATE FUNCTION nw.validate_link_node_references()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  node_geom     geometry(POINT, 3067);
BEGIN
  SELECT INTO node_geom geom FROM nw.nodes WHERE nodeid = NEW.inode;
  IF node_geom IS NULL THEN
    RAISE WARNING 'inode % of link % does not exist, link not updated', NEW.inode, NEW.linkid;
  ELSIF NOT ST_StartPoint(NEW.geom) && node_geom THEN
    RAISE WARNING 'Link % geom does not start from inode %, link not updated', NEW.linkid, NEW.inode;
    RETURN NULL;
  END IF;
  SELECT INTO node_geom geom FROM nw.nodes WHERE nodeid = NEW.jnode;
  IF node_geom IS NULL THEN
    RAISE WARNING 'jnode % of link % does not exist, link not updated', NEW.jnode, NEW.linkid;
  ELSIF NOT ST_EndPoint(NEW.geom) && node_geom THEN
    RAISE WARNING 'Link % geom does not end to jnode %, link not updated', NEW.linkid, NEW.jnode;
    RETURN NULL;
  END IF;
END;
$$;
COMMENT ON FUNCTION nw.validate_link_node_references() IS
'Ensures that the `inode` and `jnode` of the link not only exist in the node table
but their respective geometries also point to the link start and end.';

CREATE TRIGGER validate_link_node_references
BEFORE UPDATE OF inode, jnode ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.validate_link_node_references();



CREATE TABLE nw.stops (
  stopid        integer             PRIMARY KEY,
  nodeid        integer             NOT NULL,
  mode          text                NOT NULL CHECK (mode IN ('bus', 'tram')),
  code          text,
  name          text,
  descr         text,
  parent        integer
);
CREATE INDEX stops_nodeid_idx
  ON nw.stops (nodeid);
CREATE INDEX stops_mode_idx
  ON nw.stops (mode);
