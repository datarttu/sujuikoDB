DROP SCHEMA IF EXISTS nw CASCADE;
CREATE SCHEMA nw;

CREATE SEQUENCE nw.nodes_nodeid_seq AS integer;

CREATE TABLE nw.nodes (
  nodeid        integer                 PRIMARY KEY DEFAULT nextval('nw.nodes_nodeid_seq'),
  geom          geometry(POINT, 3067)   NOT NULL
);
COMMENT ON TABLE nw.nodes IS
'Represent separation points on street and track networks:
intersections, link ends, and locations where two links
with different attribute values must be separated.';

CREATE INDEX ON nw.nodes USING GIST (geom);

CREATE FUNCTION nw.keep_nodeid_seq_at_max()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
BEGIN
  PERFORM setval('nw.nodes_nodeid_seq', (SELECT coalesce(max(nodeid), 1) FROM nw.nodes));
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.keep_nodeid_seq_at_max() IS
'Updates nodeid sequence even when nodeid values are inserted or updated directly.';

CREATE TRIGGER t001_keep_nodeid_seq_at_max
AFTER INSERT OR UPDATE ON nw.nodes
FOR EACH STATEMENT EXECUTE PROCEDURE nw.keep_nodeid_seq_at_max();

-- HSL area validation trigger
CREATE FUNCTION nw.validate_node_is_within_hsl_area()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  hsl_bbox    geometry(POLYGON, 3067);
  warn_result text;
BEGIN
  hsl_bbox := ST_GeomFromText(
    'POLYGON((244592 6628611,255619 6788176,499650 6779684,499634 6619861,244592 6628611))',
    3067
  );
  IF TG_OP = 'UPDATE' THEN warn_result := 'not updated';
  ELSE warn_result := 'discarded';
  END IF;
  IF NOT NEW.geom && hsl_bbox THEN
    RAISE WARNING 'NODE % %: outside HSL area', NEW.nodeid, warn_result;
    RETURN NULL;
  ELSE RETURN NEW;
  END IF;
END;
$$;
COMMENT ON FUNCTION nw.validate_node_is_within_hsl_area() IS
'Ensures the `geom` of a new node is inside a bounding box containing the HSL area.';

CREATE TRIGGER t01_validate_node_is_within_hsl_area
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
  warn_result text;
BEGIN
  snap_tolerance := 1.0;
  IF TG_OP = 'UPDATE' THEN warn_result := 'not updated';
  ELSE warn_result := 'discarded';
  END IF;
  SELECT INTO existing_node nd.nodeid FROM nw.nodes AS nd
  WHERE ST_DWithin(NEW.geom, nd.geom, snap_tolerance)
  LIMIT 1;
  IF existing_node IS NOT NULL THEN
    RAISE WARNING 'NODE % %: is too close to existing node %', NEW.nodeid, warn_result, existing_node;
    RETURN NULL;
  ELSE RETURN NEW;
  END IF;
END;
$$;
COMMENT ON FUNCTION nw.validate_node_unique_location() IS
'Ensures the `geom` of a new node is no closer than 1 meters to an existing node.';

CREATE TRIGGER t02_validate_node_unique_location
BEFORE INSERT OR UPDATE ON nw.nodes
FOR EACH ROW EXECUTE PROCEDURE nw.validate_node_unique_location();

-- If node is moved and has attached links, automatically update the link geoms
-- so they still follow the node.
CREATE FUNCTION nw.update_attached_link_geoms()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_affected  integer;
BEGIN
  -- Links having this as inode -> keep jnode untouched
  UPDATE nw.links
  SET geom = stretch_link_from_end(geom, NEW.geom)
  WHERE inode = NEW.nodeid;
  GET DIAGNOSTICS n_affected := ROW_COUNT;
  IF n_affected > 0 THEN
    RAISE NOTICE 'NODE %: % link geoms with this as inode updated', NEW.nodeid, n_affected;
  END IF;
  -- Links having this as jnode -> keep inode untouched
  UPDATE nw.links
  SET geom = stretch_link_from_start(geom, NEW.geom)
  WHERE jnode = NEW.nodeid;
  GET DIAGNOSTICS n_affected := ROW_COUNT;
  IF n_affected > 0 THEN
    RAISE NOTICE 'NODE %: % link geoms with this as jnode updated', NEW.nodeid, n_affected;
  END IF;
  RETURN NEW;
EXCEPTION WHEN SQLSTATE '01000' THEN
  RAISE WARNING 'NODE % not updated: attached link geometries could not be updated', NEW.nodeid;
  RETURN NULL;
END;
$$;
COMMENT ON FUNCTION nw.update_attached_link_geoms() IS
'When moving a node with links attached to it, stretches and rotates the links
so they still attach to the node (as long as the new link geometries
do not break any rules).';

CREATE TRIGGER t11_update_attached_link_geoms
AFTER UPDATE OF geom ON nw.nodes
FOR EACH ROW EXECUTE PROCEDURE nw.update_attached_link_geoms();


CREATE TABLE nw.links (
  linkid        serial              PRIMARY KEY,
  inode         integer             NOT NULL REFERENCES nw.nodes(nodeid),
  jnode         integer             NOT NULL REFERENCES nw.nodes(nodeid),
  mode          text                NOT NULL CHECK (mode IN ('bus', 'tram')) DEFAULT 'bus',
  oneway        boolean             NOT NULL DEFAULT true,
  cost          double precision,
  rcost         double precision,
  attributes    jsonb,
  geom          geometry(LINESTRING, 3067)  NOT NULL,
  warnings      text
);
COMMENT ON TABLE nw.links IS
'Represent connections between nodes.';

CREATE INDEX ON nw.links (mode);
CREATE INDEX ON nw.links USING GIST (geom);

-- Geometry relationship check with existing links
CREATE FUNCTION nw.validate_geom_relationships()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  warn_result       text;
  conflicting_links text;
BEGIN
  IF TG_OP = 'UPDATE' THEN warn_result := 'not updated';
  ELSE warn_result := 'discarded';
  END IF;

  SELECT INTO conflicting_links string_agg(linkid::text, ', ' ORDER BY linkid)
  FROM nw.links
  WHERE NEW.mode = mode
    AND NEW.geom && geom
    AND ST_Intersects(NEW.geom, geom)
    AND NOT (ST_Relate(NEW.geom, geom, 'FF*F*****') OR ST_Relate(NEW.geom, geom, '0F*F*****'));

  IF conflicting_links IS NOT NULL THEN
    RAISE WARNING 'LINK % %: touches, overlaps or equals links %', NEW.linkid, warn_result, conflicting_links;
    NEW.warnings := format('touches, overlaps or equals links %s', conflicting_links);
    --RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.validate_geom_relationships IS
'Ensures that the new link geometry does not touch the edge of, overlap or equal
any existing link geometries of the same mode.';

CREATE TRIGGER t01_validate_geom_relationships
BEFORE INSERT OR UPDATE OF geom ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.validate_geom_relationships();

-- Update cost and rcost by length and oneway.
-- Geometry relationship check with existing links
CREATE FUNCTION nw.set_cost_values()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
BEGIN
  NEW.cost := ST_Length(NEW.geom);
  IF NEW.oneway THEN NEW.rcost := -1;
  ELSE NEW.rcost = NEW.cost;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.set_cost_values IS
'Sets the `cost` of a link always to the link geometry length, and `rcost` to -1
for a oneway link and to geometry length for a two-way link.';

CREATE TRIGGER t02_set_cost_values
BEFORE INSERT OR UPDATE ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.set_cost_values();

-- If there is an existing node at the link start, use it as inode,
-- if at the link end, use it as jnode.
CREATE FUNCTION nw.set_inode_ref_by_location()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  existing_nodeid integer;
BEGIN
  SELECT INTO existing_nodeid nd.nodeid FROM nw.nodes AS nd
  WHERE ST_StartPoint(NEW.geom) && nd.geom LIMIT 1;
  IF FOUND THEN
    NEW.inode := existing_nodeid;
    RAISE NOTICE 'LINK %: node % at start set as inode', NEW.linkid, existing_nodeid;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.set_inode_ref_by_location IS
'If the start of a new or updated link lies exactly at an existing node,
set that node as the inode of the link.';

CREATE TRIGGER t06_set_inode_ref_by_location
BEFORE INSERT OR UPDATE OF geom ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.set_inode_ref_by_location();

CREATE FUNCTION nw.set_jnode_ref_by_location()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  existing_nodeid integer;
BEGIN
  SELECT INTO existing_nodeid nd.nodeid FROM nw.nodes AS nd
  WHERE ST_EndPoint(NEW.geom) && nd.geom LIMIT 1;
  IF FOUND THEN
    NEW.jnode := existing_nodeid;
    RAISE NOTICE 'LINK %: node % at end set as jnode', NEW.linkid, existing_nodeid;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.set_jnode_ref_by_location IS
'If the end of a new or updated link lies exactly at an existing node,
set that node as the jnode of the link.';

CREATE TRIGGER t07_set_jnode_ref_by_location
BEFORE INSERT OR UPDATE OF geom ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.set_jnode_ref_by_location();

-- Links are stretched to reach existing nodes if they are closer than 1 m
-- to the link end.
CREATE FUNCTION nw.snap_geom_to_inode()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  existing_node record;
BEGIN
  SELECT INTO existing_node nd.* FROM nw.nodes AS nd
  WHERE ST_DWithin(ST_StartPoint(NEW.geom), nd.geom, 1.0)
    AND NOT ST_StartPoint(NEW.geom) && nd.geom
  LIMIT 1;
  IF FOUND THEN
    NEW.geom := stretch_link_from_end(NEW.geom, existing_node.geom);
    NEW.inode := existing_node.nodeid;
    RAISE NOTICE 'LINK %: stretched to existing inode %', NEW.linkid, existing_node.nodeid;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.snap_geom_to_inode IS
'If the start of a new or updated link lies closer than 1 m to an existing node,
stretch the link to snap the start to that node.';

CREATE TRIGGER t11_snap_geom_to_inode
BEFORE INSERT OR UPDATE OF geom ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.snap_geom_to_inode();

CREATE FUNCTION nw.snap_geom_to_jnode()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  existing_node record;
BEGIN
  SELECT INTO existing_node nd.* FROM nw.nodes AS nd
  WHERE ST_DWithin(ST_EndPoint(NEW.geom), nd.geom, 1.0)
    AND NOT ST_EndPoint(NEW.geom) && nd.geom
  LIMIT 1;
  IF FOUND THEN
    NEW.geom := stretch_link_from_end(NEW.geom, existing_node.geom);
    NEW.jnode := existing_node.nodeid;
    RAISE NOTICE 'LINK %: stretched to existing jnode %', NEW.linkid, existing_node.nodeid;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.validate_geom_relationships IS
'If the end of a new or updated link lies closer than 1 m to an existing node,
stretch the link to snap the end to that node.';

CREATE TRIGGER t12_snap_geom_to_jnode
BEFORE INSERT OR UPDATE OF geom ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.snap_geom_to_jnode();

-- If there is no existing node at the link start or close to it,
-- create one and set is as inode of the link.
-- The same applies to the link end and jnode.
CREATE FUNCTION nw.add_missing_inode()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  nodeid_created  integer;
BEGIN
  IF NEW.inode IS NULL AND NOT EXISTS (
    SELECT nodeid FROM nw.nodes
    WHERE ST_StartPoint(NEW.geom) && geom
  ) THEN
    INSERT INTO nw.nodes(geom) VALUES (ST_StartPoint(NEW.geom))
    RETURNING nodeid INTO nodeid_created;
    NEW.inode := nodeid_created;
    RAISE NOTICE 'LINK %: created new node % as inode', NEW.linkid, nodeid_created;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.add_missing_inode IS
'If a new / modified link does not have an existing `inode` at its start, create one.';

CREATE TRIGGER t21_add_missing_inode
BEFORE INSERT OR UPDATE OF geom ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.add_missing_inode();

CREATE FUNCTION nw.add_missing_jnode()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  nodeid_created  integer;
BEGIN
  IF NEW.jnode IS NULL AND NOT EXISTS (
    SELECT nodeid FROM nw.nodes
    WHERE ST_EndPoint(NEW.geom) && geom
  ) THEN
    INSERT INTO nw.nodes(geom) VALUES (ST_EndPoint(NEW.geom))
    RETURNING nodeid INTO nodeid_created;
    NEW.jnode := nodeid_created;
    RAISE NOTICE 'LINK %: created new node % as jnode', NEW.linkid, nodeid_created;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.add_missing_jnode IS
'If a new / modified link does not have an existing `jnode` at its end, create one.';

CREATE TRIGGER t22_add_missing_jnode
BEFORE INSERT OR UPDATE OF geom ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.add_missing_jnode();

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
    RAISE WARNING 'LINK % not updated: inode % does not exist', NEW.linkid, NEW.inode;
  ELSIF NOT ST_StartPoint(NEW.geom) && node_geom THEN
    RAISE WARNING 'LINK % not updated: geom does not start from inode %', NEW.linkid, NEW.inode;
    RETURN NULL;
  END IF;
  SELECT INTO node_geom geom FROM nw.nodes WHERE nodeid = NEW.jnode;
  IF node_geom IS NULL THEN
    RAISE WARNING 'LINK % not updated: jnode % does not exist', NEW.linkid, NEW.jnode;
  ELSIF NOT ST_EndPoint(NEW.geom) && node_geom THEN
    RAISE WARNING 'LINK % not updated: geom does not end to jnode %', NEW.linkid, NEW.jnode;
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION nw.validate_link_node_references() IS
'Ensures that the `inode` and `jnode` of the link not only exist in the node table
but their respective geometries also point to the link start and end.';

CREATE TRIGGER t31_validate_link_node_references
BEFORE UPDATE OF inode, jnode ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.validate_link_node_references();

-- inode may not equal jnode.
-- We could use a CHECK constraint, but it would result in an error.
-- Instead, we just want to ignore the failing feature and warn about it.
CREATE FUNCTION nw.validate_link_inode_jnode_not_eq()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
DECLARE
  warn_result text;
BEGIN
  IF TG_OP = 'UPDATE' THEN warn_result := 'not updated';
  ELSE warn_result := 'discarded';
  END IF;
  IF NEW.inode = NEW.jnode THEN
    RAISE WARNING 'LINK % %: inode % must not equal jnode %', NEW.linkid, warn_result, NEW.inode, NEW.jnode;
    RETURN NULL;
  ELSE RETURN NEW;
  END IF;
END;
$$;
COMMENT ON FUNCTION nw.validate_link_inode_jnode_not_eq() IS
'Ensures that the `inode` and `jnode` of the link are not the same node.';

CREATE TRIGGER t32_validate_link_inode_jnode_not_eq
BEFORE INSERT OR UPDATE ON nw.links
FOR EACH ROW EXECUTE PROCEDURE nw.validate_link_inode_jnode_not_eq();



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
