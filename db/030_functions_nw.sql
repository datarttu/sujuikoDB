/*
 * Functions and procedures related to nw schema.
 * Note that trigger functions are not declared here,
 * instead they are in 020_schema_nw.sql with their relations.
 */

/*
 * # Updating link references to nw.stop records
 *
 * The first function finds the nearest link for each stop point
 * and the relative location along the link,
 * and returns a set of the results (so it can be used without side effects).
 * Stops marked with link_ref_manual = true and further than max_distance_m away
 * from the nearest link are not considered.
 * In case of two-way links, the correct direction to use is determined
 * such that the stop shall lie on the right-hand side of the link,
 * viewed from the link start to end node.
 *
 * Why we are using sided ST_Buffer: see https://github.com/datarttu/sujuikoDB/issues/70
 *
 * The second procedure is just for calling the first one and saving the results
 * to the actual nw.stop table by stop_id.
 * Example:
 *
 * > CALL nw.update_stop_link_refs(30.0);
 */

CREATE FUNCTION nw.get_stop_link_refs(
  max_distance_m float8 DEFAULT 20.0
)
RETURNS TABLE (
    stop_id             integer,
    link_id             integer,
    link_reversed       boolean,
    location_on_link    float8,
    distance_from_link  float8
)
STABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT
    st.stop_id,
    li.link_id,
    li.link_reversed,
    li.location_on_link,
    li.distance_from_link
  FROM nw.stop AS st
  INNER JOIN LATERAL (
    SELECT
      vld.link_id,
      vld.link_reversed,
      ST_LineLocatePoint(vld.geom, st.geom) AS location_on_link,
      ST_Distance(vld.geom, st.geom)        AS distance_from_link
    FROM nw.view_link_directed AS vld
    WHERE ST_DWithin(vld.geom, st.geom, max_distance_m)
      AND ARRAY[st.stop_mode] <@ vld.link_modes
      AND ST_Contains(
        ST_Buffer(vld.geom, max_distance_m, 'side=right'),
        st.geom)
    ORDER BY ST_Distance(vld.geom, st.geom)
    LIMIT 1
  ) AS li
  ON true;
$$;

COMMENT ON FUNCTION nw.get_stop_link_refs IS
'Returns directed link reference candidates for stops. A stop must lie on the right-hand side of the link, max. max_distance_m away from it, and the closest link candidate is returned.';

CREATE PROCEDURE nw.update_stop_link_ref(
  target_stop_id  integer,
  max_distance_m  float8 DEFAULT 20.0
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_total   integer;
  n_manual  integer;
  n_updated integer;
BEGIN
  SELECT INTO n_total count(*) FROM nw.stop WHERE stop_id = target_stop_id;
  SELECT INTO n_manual count(*) FROM nw.stop WHERE link_ref_manual AND stop_id = target_stop_id;

  WITH updated AS (
    UPDATE nw.stop AS st
    SET
      link_id = upd.link_id,
      link_reversed = upd.link_reversed,
      location_on_link = upd.location_on_link,
      distance_from_link = upd.distance_from_link
    FROM (
      SELECT * FROM nw.get_stop_link_refs(max_distance_m := max_distance_m)
      WHERE stop_id = target_stop_id
    ) AS upd
    WHERE st.stop_id = target_stop_id
      AND st.stop_id = upd.stop_id
      AND NOT st.link_ref_manual
    RETURNING 1
  )
  SELECT INTO n_updated count(*)
  FROM updated;

  RAISE INFO '% nw.stop records updated (total % stops, % protected from updates)',
    n_updated, n_total, n_manual;
END;
$$;

COMMENT ON PROCEDURE nw.update_stop_link_ref IS
'Updates link references of a single stop by `target_stop_id`.';

CREATE PROCEDURE nw.batch_update_stop_link_refs(
  max_distance_m float8 DEFAULT 20.0
)
LANGUAGE PLPGSQL
AS $procedure$
DECLARE
  n_total   integer;
  n_manual  integer;
  n_updated integer;
BEGIN
  SELECT INTO n_total count(*) FROM nw.stop;
  SELECT INTO n_manual count(*) FROM nw.stop WHERE link_ref_manual;

  WITH updated AS (
    UPDATE nw.stop AS st
    SET
      link_id = upd.link_id,
      link_reversed = upd.link_reversed,
      location_on_link = upd.location_on_link,
      distance_from_link = upd.distance_from_link
    FROM (
      SELECT * FROM nw.get_stop_link_refs(max_distance_m := max_distance_m)
    ) AS upd
    WHERE st.stop_id = upd.stop_id
      AND NOT st.link_ref_manual
    RETURNING 1
  )
  SELECT INTO n_updated count(*)
  FROM updated;

  RAISE INFO '% nw.stop records updated (total % stops, % protected from updates)',
    n_updated, n_total, n_manual;
END;
$procedure$;

COMMENT ON PROCEDURE nw.batch_update_stop_link_refs IS
'Updates link references of all stops not marked with `link_ref_manual = true`.';

/*
 * # Routing (generating link_on_route and link_on_section results)
 *
 * We have separate nw.parts_dijkstra_via_nodes and a parent function
 * nw.dijkstra_via_nodes, because we do not have a handy way to add
 * a "global" link_seq running number right away when loop-returning
 * the pgr_dijkstra result sets along via_nodes 1-2, 2-3, etc.
 * Also we cannot return a completely empty set from the loop if there is even
 * one node pair without a path found (pgr_dijkstra returns an empty set
 * in such cases): instead, we can raise an exception, catch it in the parent
 * function and in that case return an empty set from the parent.
 *
 * Example:
 * > SELECT * FROM nw.dijkstra_via_nodes(ARRAY[233, 176, 197]);
 */

CREATE FUNCTION nw.parts_dijkstra_via_nodes(via_nodes integer[])
RETURNS TABLE (
  part_seq      integer,
  link_sub_seq  integer,
  link_id       integer,
  link_reversed boolean
)
STABLE
PARALLEL SAFE
LANGUAGE PLPGSQL
AS $$
DECLARE
  i integer;
BEGIN
  IF cardinality(via_nodes) < 2 THEN
    RAISE EXCEPTION 'via_nodes must contain at least 2 nodes';
  END IF;

  i := 1;

  WHILE i < cardinality(via_nodes)
  LOOP
    RETURN QUERY
      SELECT
        i                 AS part_seq,
        pd.seq            AS link_sub_seq,
        li.link_id        AS link_id,
        li.link_reversed  AS link_reversed
      FROM pgr_dijkstra(
        'SELECT uniq_link_id AS id, i_node AS source, j_node AS target, length_m AS cost FROM nw.view_link_directed',
        via_nodes[i],
        via_nodes[i+1]
        ) AS pd
      INNER JOIN nw.view_link_directed AS li
        ON pd.edge = li.uniq_link_id
      WHERE pd.edge <> -1;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'No path between nodes % and %',
        via_nodes[i], via_nodes[i+1]
        USING ERRCODE = 'no_data_found';
      EXIT;
    END IF;

    i := i + 1;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION nw.parts_dijkstra_via_nodes IS
'Runs Dijkstra routing for <via_node_n>, <via_node_n+1> pairs and returns paths with `part_seq` identifying the pair order number.';

CREATE FUNCTION nw.dijkstra_via_nodes(via_nodes integer[])
RETURNS TABLE (
  link_seq        integer,
  link_id         integer,
  link_reversed   boolean
)
STABLE
PARALLEL SAFE
LANGUAGE PLPGSQL
AS $$
DECLARE
  msg text;
BEGIN
  RETURN QUERY
    WITH part_paths AS (
      SELECT
        row_number() OVER (ORDER BY pd.part_seq, pd.link_sub_seq) AS link_seq,
        pd.part_seq,
        pd.link_sub_seq,
        pd.link_id,
        pd.link_reversed
      FROM nw.parts_dijkstra_via_nodes(via_nodes := via_nodes) AS pd
    )
    SELECT pp.link_seq::integer, pp.link_id, pp.link_reversed
    FROM part_paths AS pp;

EXCEPTION WHEN no_data_found THEN
  GET STACKED DIAGNOSTICS msg := MESSAGE_TEXT;
  RAISE NOTICE '%', msg;
  RETURN;
END;
$$;

COMMENT ON FUNCTION nw.dijkstra_via_nodes IS
'Runs Dijkstra routing using an ordered array of via-nodes, and returns the path as set of ordered link references.';

/*
 * # Creating and updating sections (for analysis)
 *
 * "upsert" = if links on section of target_section_id already exist,
 * delete them and create new ones.
 * Some notes:
 * - Section target_section_id must be present in nw.section
 * - If via_nodes in nw.section is empty or differs from target_via_nodes,
 *   it is updated
 * - Empty routing result does NOT interrupt the update process at the moment,
 *   old rows are still deleted from nw.link_on_section.
 *   The user must re-run this function with different via_nodes if needed.
 *   (Interrupting would require storing the nw.dijkstra_via_nodes result
 *   temporarily and checking if it has > 0 rows.)
 */

CREATE PROCEDURE nw.upsert_links_on_section(
  target_section_id text,
  target_via_nodes  integer[]
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_results integer;
BEGIN

  IF NOT EXISTS (SELECT * FROM nw.section WHERE section_id = target_section_id) THEN
    RAISE INFO 'section_id % not in nw.section: skipping', target_section_id;
    RETURN;
  END IF;

  WITH deleted AS (
    DELETE FROM nw.link_on_section
    WHERE section_id = target_section_id
    RETURNING 1
  )
  SELECT INTO n_results count(*) FROM deleted;
  IF n_results > 0 THEN
    RAISE INFO '% old links on section % deleted',
      n_results, target_section_id;
  END IF;

  WITH inserted AS (
    INSERT INTO nw.link_on_section (section_id, link_seq, link_id, link_reversed)
    SELECT target_section_id, dvn.link_seq, dvn.link_id, dvn.link_reversed
    FROM nw.dijkstra_via_nodes(via_nodes := target_via_nodes) AS dvn
    RETURNING 1
  )
  SELECT INTO n_results count(*) FROM inserted;
  RAISE INFO '% new links on section % created',
    n_results, target_section_id;

  IF (
    SELECT coalesce(via_nodes, ARRAY[0]) <> target_via_nodes
    FROM nw.section
    WHERE section_id = target_section_id
  ) THEN
    UPDATE nw.section
    SET via_nodes = target_via_nodes
    WHERE section_id = target_section_id;
    RAISE INFO '{%} updated as via_nodes of %',
      array_to_string(target_via_nodes, ','),
      target_section_id;
  END IF;

END;
$$;

COMMENT ON PROCEDURE nw.upsert_links_on_section IS
'Creates / updates the link path of a section `target_section_id` using Dijkstra routing with `via_nodes`. If `via_nodes` are given in this procedure, they are also updated to the `nw.section.via_nodes`.';

/*
 * The following procedure will mostly be run right after data imports:
 * after that, the user should check through the sections
 * and re-run nw.upsert_links_on_section() for the ones that need revisions.
 * It is not very elegant that we loop through the sections and then within
 * each step we check again if the section_id exists in the table, etc.
 * but the performance overhead is not significant since this procedure is not
 * run very often and the number of sections will probably be quite moderate.
 */

CREATE PROCEDURE nw.batch_upsert_links_on_section()
LANGUAGE PLPGSQL
AS $$
DECLARE
  rec record;
BEGIN
  FOR rec IN (
    SELECT section_id, via_nodes
    FROM nw.section
    WHERE cardinality(via_nodes) > 1
  ) LOOP
    CALL nw.upsert_links_on_section(
      target_section_id := rec.section_id,
      target_via_nodes := rec.via_nodes
    );
  END LOOP;
END;
$$;

COMMENT ON PROCEDURE nw.batch_upsert_links_on_section IS
'Creates / updates link paths for all sections.';

/*
 * This procedure updates the section rotation value in degrees
 * with the azimuth between the start and end nodes of the section.
 * Note that it uses the via_nodes, NOT the links_on_section geometries.
 */
CREATE PROCEDURE nw.batch_update_section_rotation()
LANGUAGE PLPGSQL
AS $procedure$
DECLARE
  n_updated bigint;
BEGIN
  WITH rotations AS (
    SELECT
      se.section_id,
      degrees(
        ST_Azimuth(ind.geom, jnd.geom)
      ) AS rotation
    FROM nw.section AS se
    INNER JOIN nw.node  AS ind
      ON (se.via_nodes[1] = ind.node_id)
    INNER JOIN nw.node  AS jnd
      ON (se.via_nodes[cardinality(se.via_nodes)] = jnd.node_id)
    WHERE cardinality(se.via_nodes) > 1
  ),
  updated AS (
    UPDATE nw.section AS upd
    SET rotation = rt.rotation
    FROM rotations AS rt
    WHERE upd.section_id = rt.section_id
    RETURNING 1
  )
  SELECT INTO n_updated count(*) FROM updated;
  RAISE INFO '% section rotation values updated', n_updated;
END;
$procedure$;

COMMENT ON PROCEDURE nw.batch_update_section_rotation IS
'Sets rotation values for `nw.section` by the azimuth between start and end nodes of the section.';

/*
 * # Creating and updating nw.link_on_route route version paths
 *
 * This works with the same idea as the sections and links on section above,
 * except via nodes cannot be passed directly to the procedure.
 * Instead, they must be available through nw.view_vianode_on_route,
 * from where they are made into an array to pass to the routing algorithm.
 * User interaction is a bit different here than in section via nodes:
 * stop_on_route stops must be located in a valid way on the network such
 * that they can be used for via nodes anyway, and if the user wants,
 * they can add their own via nodes by nw.manual_vianode_on_route.
 *
 * Again, this is decomposed to a single route version procedure and a parent
 * procedure calling it in a loop for all route versions.
 * The latter can be run first, then individual route version paths are checked
 * and the ones with errors can be fixed by adding manual via nodes and
 * re-running nw.upsert_links_on_route() for individual route versions.
 */

CREATE PROCEDURE nw.upsert_links_on_route(
  target_route_ver_id text
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  target_rec  record;
  n_results   integer;
BEGIN

  IF NOT EXISTS (SELECT * FROM nw.route_version WHERE route_ver_id = target_route_ver_id) THEN
    RAISE NOTICE 'route_ver_id % not in nw.route_ver_id: skipping', target_route_ver_id;
    RETURN;
  END IF;

  SELECT
    route_ver_id,
    bool_or(node_id IS NULL)              AS has_null_nodes,
    array_agg(node_id ORDER BY node_seq)  AS via_nodes
  INTO target_rec
  FROM nw.view_vianode_on_route
  WHERE route_ver_id = target_route_ver_id
  GROUP BY route_ver_id;

  IF target_rec.via_nodes IS NULL THEN
    RAISE NOTICE 'No via nodes for % in nw.view_vianode_on_route: skipping', target_route_ver_id;
    RETURN;
  END IF;

  IF target_rec.has_null_nodes THEN
    RAISE NOTICE '% has NULL via nodes (see nw.view_vianode_on_route): skipping', target_route_ver_id;
    RETURN;
  END IF;

  WITH deleted AS (
    DELETE FROM nw.link_on_route
    WHERE route_ver_id = target_route_ver_id
    RETURNING 1
  )
  SELECT INTO n_results count(*) FROM deleted;
  IF n_results > 0 THEN
    RAISE INFO '% old links on route % deleted',
      n_results, target_route_ver_id;
  END IF;

  WITH inserted AS (
    INSERT INTO nw.link_on_route (route_ver_id, link_seq, link_id, link_reversed)
    SELECT target_route_ver_id, dvn.link_seq, dvn.link_id, dvn.link_reversed
    FROM nw.dijkstra_via_nodes(via_nodes := target_rec.via_nodes) AS dvn
    RETURNING 1
  )
  SELECT INTO n_results count(*) FROM inserted;
  RAISE INFO '% new links on route % created',
    n_results, target_route_ver_id;

END;
$$;

COMMENT ON PROCEDURE nw.upsert_links_on_route IS
'Creates / updates `link_on_route` path of `target_route_ver_id` with Dijkstra routing, using via nodes from `nw.view_vianode_on_route`.';

CREATE PROCEDURE nw.batch_upsert_links_on_route()
LANGUAGE PLPGSQL
AS $$
DECLARE
  rec record;
BEGIN
  FOR rec IN (
    SELECT route_ver_id
    FROM nw.route_version
  ) LOOP
    CALL nw.upsert_links_on_route(
      target_route_ver_id := rec.route_ver_id
    );
  END LOOP;
END;
$$;

COMMENT ON PROCEDURE nw.batch_upsert_links_on_route IS
'Creates / updates `link_on_route` paths for all route versions.';
