/*
 * # MATCHING HFP POINTS TO LINKS ON ROUTE
 *
 * HFP (GPS) points are projected to the closest link on the corresponding
 * route version.
 * The subset of suitable links_on_route is found using jrnid -> route_ver_id.
 *
 * The get_point_on_link_candidates function finds all links_on_route that
 * enclose the HFP point within their buffer of max_distance_m:
 * this is a _side_ buffer, so points are not matched around the link ends!
 * So, get_point_on_link_candidates may well return several link location
 * matches per HFP point, most often around intersections.
 * This is why it returns "candidates".
 *
 * In the create_points_on_link procedure we then take from each projected point
 * candidate the one that has the smallest distance_from_link, so we have
 * unique (jrnid, tst) rows to insert to obs.point_on_link.
 *
 * TODO: This is a very simple algorithm but it will fail at many spots in the
 *       network, because it does not consider HFP point order in time vs.
 *       route on link order at all. In other words, some points that get simply
 *       matched to the geographically closest link may then be misplaced on the
 *       resulting path along the network, for instance, if the same link has
 *       been used several times as link_on_route.
 *       The algorithm should therefore be developed further to eliminate
 *       "artificial" backing up of HFP points along the links_on_route
 *       and to ensure realistic space-time measures on links and sections.
 *
 * Inserting the results into obs.point_on_link is implemented with per-journey
 * procedure + batch procedure (again, cf. nw.upsert_links_on_route()).
 * This appears at bit odd, but this way it is easier to monitor how HFP journeys
 * behave in the processing and re-run individual journeys if required.
 * It does not seem to have almost any run time overhead at all either, compared
 * to a simple INSERT INTO clause without WHERE.
 */

CREATE FUNCTION obs.get_point_on_link_candidates(
  max_distance_m float8 DEFAULT 20.0
)
RETURNS TABLE (
  jrnid               uuid,
  tst                 timestamptz,
  link_seq            integer,
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
  hfp.jrnid,
  hfp.tst,
  lor.link_seq,
  lor.link_id,
  lor.link_reversed,
  abs(
    ST_LineLocatePoint(li.geom, hfp.geom) - (lor.link_reversed::integer)
  )                                       AS location_on_link,
  ST_Distance(li.geom, hfp.geom)          AS distance_from_link
FROM obs.hfp_point          AS hfp
INNER JOIN obs.journey      AS jrn
  ON (hfp.jrnid = jrn.jrnid)
INNER JOIN nw.link_on_route AS lor
  ON (jrn.route_ver_id = lor.route_ver_id)
INNER JOIN nw.link          AS li
  ON (
    lor.link_id = li.link_id
    AND ST_DWithin(hfp.geom, li.geom, $1)
    AND ST_Within(hfp.geom, ST_Buffer(li.geom, $1, 'endcap=flat'))
  );
$$;

CREATE PROCEDURE obs.create_points_on_link(
  target_jrnid    uuid,
  max_distance_m  float8  DEFAULT 20.0
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  n_source_points bigint;
  n_result_points bigint;
BEGIN
  SELECT INTO n_source_points count(1)
  FROM obs.hfp_point
  WHERE jrnid = target_jrnid;

  IF NOT FOUND THEN
    RAISE INFO 'jrnid %: no hfp points', target_jrnid;
    RETURN;
  END IF;

  WITH inserted AS (
    INSERT INTO obs.point_on_link(jrnid, tst, link_seq, link_id, link_reversed, location_on_link, distance_from_link)
    SELECT DISTINCT ON (jrnid, tst) jrnid, tst, link_seq, link_id, link_reversed, location_on_link, distance_from_link
    FROM obs.get_point_on_link_candidates(20.0)
    WHERE jrnid = target_jrnid
    ORDER BY jrnid, tst, distance_from_link
    ON CONFLICT DO NOTHING
    RETURNING 1
  )
  SELECT INTO n_result_points count(1) FROM inserted;

  RAISE INFO 'jrnid %: % hfp_point -> % point_on_link',
    target_jrnid, n_source_points, n_result_points;
END;
$$;

CREATE PROCEDURE obs.batch_create_points_on_link(
  max_distance_m  float8  DEFAULT 20.0
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  rec record;
BEGIN
  FOR rec IN (
    SELECT jrnid
    FROM obs.journey
  ) LOOP
    CALL obs.create_points_on_link(
      target_jrnid := rec.jrnid
    );
  END LOOP;
END;
$$;
