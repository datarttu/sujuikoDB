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
    ST_LineLocatePoint(li.geom, st.geom)  AS location_on_link,
    ST_Distance(li.geom, st.geom)         AS distance_from_link
  FROM nw.stop AS st
  INNER JOIN nw.view_link_directed AS li
    ON ST_DWithin(st.geom, li.geom, max_distance_m)
  ORDER BY st.stop_id, ST_Distance(st.geom, li.geom);
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

  -- TODO: Invoke the above function inside an UPDATE statement
  n_updated := 0;
  
  RAISE INFO '% nw.stop records updated (total % stops, % protected from updates)',
    n_updated, n_total, n_updated;
END;
$$;
