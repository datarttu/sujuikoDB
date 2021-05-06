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
    link_dir            smallint,
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
    li.link_dir,
    li.location_on_link,
    li.distance_from_link
  FROM nw.stop AS st
  INNER JOIN LATERAL (
    SELECT
      vld.link_id,
      vld.link_dir,
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

CREATE PROCEDURE nw.update_stop_link_refs(
  max_distance_m float8 DEFAULT 20.0
)
LANGUAGE PLPGSQL
AS $$
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
      link_dir = upd.link_dir,
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
$$;

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
  link_dir      smallint
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
        i           AS part_seq,
        pd.seq      AS link_sub_seq,
        li.link_id  AS link_id,
        li.link_dir AS link_dir
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

CREATE FUNCTION nw.dijkstra_via_nodes(via_nodes integer[])
RETURNS TABLE (
  link_seq  integer,
  link_id   integer,
  link_dir  smallint
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
        row_number() OVER () AS link_seq,
        pd.part_seq,
        pd.link_sub_seq,
        pd.link_id,
        pd.link_dir
      FROM nw.parts_dijkstra_via_nodes(via_nodes := via_nodes) AS pd
      ORDER BY part_seq, link_sub_seq
    )
    SELECT pp.link_seq::integer, pp.link_id, pp.link_dir
    FROM part_paths AS pp;

EXCEPTION WHEN no_data_found THEN
  GET STACKED DIAGNOSTICS msg := MESSAGE_TEXT;
  RAISE NOTICE '%', msg;
  RETURN;
END;
$$;

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
    INSERT INTO nw.link_on_section (section_id, link_seq, link_id, link_dir)
    SELECT target_section_id, dvn.link_seq, dvn.link_id, dvn.link_dir
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
