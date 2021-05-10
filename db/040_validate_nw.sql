/*
 * # nw VALIDATIONS
 *
 * Validation is done with
 * 1) set-returning functions by rule that return rows fulfilling the criteria
 * 2) master procedures that update the errors field of the target table
 *    for rows found by the set-returning functions by primary key
 */

/*
 * # NODES
 */

CREATE FUNCTION nw.vld_node_duplicated_node()
RETURNS SETOF nw.node
STABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT a.*
  FROM nw.node        AS a
  INNER JOIN nw.node  AS b
  ON (a.geom && b.geom AND a.node_id <> b.node_id);
$$;

CREATE PROCEDURE nw.validate_nodes()
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_invalid integer DEFAULT 0;
BEGIN

  UPDATE nw.node SET errors = NULL;
  RAISE INFO 'Validate nw.node: errors reset';

  WITH updated AS (
    UPDATE nw.node AS upd
    SET errors = append_unique(errors, 'duplicated_node')
    FROM (SELECT node_id FROM nw.vld_node_duplicated_node()) AS res
    WHERE upd.node_id = res.node_id
    RETURNING 1
  )
  SELECT INTO n_invalid count(*) FROM updated;

  RAISE INFO 'Validate nw.node: % duplicated_node', n_invalid;

END;
$$;

/*
 * # LINKS
 */

CREATE FUNCTION nw.vld_link_i_node_mismatch()
RETURNS SETOF nw.link
STABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT li.*
  FROM nw.link        AS li
  INNER JOIN nw.node  AS nd
  ON (li.i_node = nd.node_id)
  WHERE NOT ST_StartPoint(li.geom) && nd.geom;
$$;

CREATE FUNCTION nw.vld_link_j_node_mismatch()
RETURNS SETOF nw.link
STABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT li.*
  FROM nw.link        AS li
  INNER JOIN nw.node  AS nd
  ON (li.j_node = nd.node_id)
  WHERE NOT ST_EndPoint(li.geom) && nd.geom;
$$;

CREATE FUNCTION nw.vld_link_start_end_same_point()
RETURNS SETOF nw.link
STABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT *
  FROM nw.link
  WHERE i_node = j_node;
$$;

CREATE PROCEDURE nw.validate_links()
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_invalid integer DEFAULT 0;
BEGIN

  UPDATE nw.link SET errors = NULL;
  RAISE INFO 'Validate nw.link: errors reset';

  WITH updated AS (
    UPDATE nw.link AS upd
    SET errors = append_unique(errors, 'i_node_mismatch')
    FROM (SELECT link_id FROM nw.vld_link_i_node_mismatch()) AS res
    WHERE upd.link_id = res.link_id
    RETURNING 1
  )
  SELECT INTO n_invalid count(*) FROM updated;
  RAISE INFO 'Validate nw.link: % i_node_mismatch', n_invalid;

  WITH updated AS (
    UPDATE nw.link AS upd
    SET errors = append_unique(errors, 'j_node_mismatch')
    FROM (SELECT link_id FROM nw.vld_link_j_node_mismatch()) AS res
    WHERE upd.link_id = res.link_id
    RETURNING 1
  )
  SELECT INTO n_invalid count(*) FROM updated;
  RAISE INFO 'Validate nw.link: % j_node_mismatch', n_invalid;

  WITH updated AS (
    UPDATE nw.link AS upd
    SET errors = append_unique(errors, 'start_end_same_point')
    FROM (SELECT link_id FROM nw.vld_link_start_end_same_point()) AS res
    WHERE upd.link_id = res.link_id
    RETURNING 1
  )
  SELECT INTO n_invalid count(*) FROM updated;
  RAISE INFO 'Validate nw.link: % start_end_same_point', n_invalid;

END;
$$;

/*
 * # STOPS
 */

CREATE FUNCTION nw.vld_stop_no_link_ref()
RETURNS SETOF nw.stop
STABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT *
  FROM nw.stop
  WHERE link_id IS NULL;
$$;

CREATE FUNCTION nw.vld_stop_incomplete_link_ref()
RETURNS SETOF nw.stop
STABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT *
  FROM nw.stop
  WHERE link_id IS NOT NULL
    AND (link_dir IS NULL OR location_on_link IS NULL OR distance_from_link IS NULL);
$$;

CREATE PROCEDURE nw.validate_stops()
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_invalid integer DEFAULT 0;
BEGIN

  UPDATE nw.stop SET errors = NULL;
  RAISE INFO 'Validate nw.stop: errors reset';

  WITH updated AS (
    UPDATE nw.stop AS upd
    SET errors = append_unique(errors, 'no_link_ref')
    FROM (SELECT stop_id FROM nw.vld_stop_no_link_ref()) AS res
    WHERE upd.stop_id = res.stop_id
    RETURNING 1
  )
  SELECT INTO n_invalid count(*) FROM updated;
  RAISE INFO 'Validate nw.stop: % no_link_ref', n_invalid;

  WITH updated AS (
    UPDATE nw.stop AS upd
    SET errors = append_unique(errors, 'incomplete_link_ref')
    FROM (SELECT stop_id FROM nw.vld_stop_incomplete_link_ref()) AS res
    WHERE upd.stop_id = res.stop_id
    RETURNING 1
  )
  SELECT INTO n_invalid count(*) FROM updated;
  RAISE INFO 'Validate nw.stop: % incomplete_link_ref', n_invalid;

END;
$$;
