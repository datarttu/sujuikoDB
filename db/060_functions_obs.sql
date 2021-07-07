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
AS $procedure$
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
$procedure$;

CREATE PROCEDURE obs.batch_create_points_on_link(
  max_distance_m  float8  DEFAULT 20.0
)
LANGUAGE PLPGSQL
AS $procedure$
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
$procedure$;

/*
 * Creating NON-MOVEMENT EVENTS, i.e. the halt_on_journey data
 *
 * The following function is based on some important assumptions:
 * firstly, when the vehicle has not moved, the HFP signal has been really
 * locked into one position (at least in post-processing of HFP before
 * db import), and there is a new HFP point whenever the vehicle stops,
 * changes its door status, or starts to move.
 * Secondly, it is assumed that the door sensor works correctly;
 * if drst is just NULL, then that door time remains unknown, but it is more
 * dangerous if the drst is accidentally inverted (which has happened).
 * Thirdly, since we use points_on_link, any HFP points left without a link
 * match are ignored, so that these result may underestimate total halt times
 * of entire journeys, for instance.
 *
 * Now the CTE expression below is rather complex and multi-staged,
 * mainly due to the separation of
 * 1) total time of the halt event, that is, the difference between the
 *    first timestamp of the event and the next timestamp
 *    when the vehicle moves again; and
 * 2) represented time, which originates from the represents_time_s attribute
 *    of the original HFP data and tells the duration of _valid_ raw data points
 *    that the point in question represents.
 * By comparing these two, we can figure out the "gaps", i.e. how much raw data
 * has been lost at some stage, either in raw HFP preparation before db,
 * or when matching hfp_point data to route links.
 *
 * To explain the CTE a bit:
 * - First we compare successive locations along route geometry (link_seq, location_on_link)
 *   to determine the points that do not move. At this point, we also get to save
 *   the timestamp of the next moving point through end_tst.
 * - Then we leave out the moving points, now having a couple of rows for each halt event
 *   (start and end of the halt, as well as door status changes).
 *   By comparing consecutive rows from these, we can mark the beginning of each
 *   halt event with 1 (new_halt_marker), 0 otherwise.
 * - Then we take the cumulative sum of the new_halt_markers -> unique halt group
 *   number within each jrnid. Yes, we could just group by link_seq and location_on_link,
 *   but as long as we allow backing up along the route geometry, this would impose
 *   the risk of grouping multiple halt events at the same location together.
 * - Finally we get to calculate totals by halt event by using the halt_group.
 * - However, since the original HFP points are unique by jrnid, tst,
 *   so are also the halt events by jrnid, min(tst), and these can be used
 *   to join these events back to the HFP data, so we no longer need
 *   the halt_group attribute in the result set.
 *
 * We do not want to do this import with a batch import
 * procedure only, since we might well want to use this function flexibly
 * to import halts from a subset of journeys only, for instance.
 */

CREATE FUNCTION obs.get_halts_on_journey(min_halt_duration_s float4 DEFAULT 3.0)
RETURNS SETOF obs.halt_on_journey
LANGUAGE SQL
AS $$
  WITH
  successive_points AS (
    SELECT
      pol.jrnid,
      pol.tst,
      lead(pol.tst) OVER w  AS end_tst,
      hp.drst,
      hp.represents_time_s,
      pol.link_seq,
      pol.location_on_link  AS link_loc,
      (
        (pol.link_seq = lead(pol.link_seq) OVER w AND pol.location_on_link = lead(pol.location_on_link) OVER w)
        OR
        (pol.link_seq = lag(pol.link_seq) OVER w AND pol.location_on_link = lag(pol.location_on_link) OVER w)
      )                     AS is_halted_point
    FROM obs.hfp_point            AS hp
    INNER JOIN obs.point_on_link  AS pol
      ON (hp.jrnid = pol.jrnid AND hp.tst = pol.tst)
    WINDOW w AS (PARTITION BY pol.jrnid ORDER BY pol.tst)
  ),
  halt_points AS (
    SELECT
      jrnid, tst, end_tst, drst, represents_time_s,
      coalesce(
        link_seq <> lag(link_seq) OVER w AND link_loc <> lag(link_loc) OVER w, true
        )::integer      AS new_halt_marker
    FROM successive_points
    WHERE is_halted_point
    WINDOW w AS (PARTITION BY jrnid ORDER BY tst)
  ),
  grouped_halt_points AS (
    SELECT
      jrnid, tst, end_tst, drst, represents_time_s,
      sum(new_halt_marker) OVER w AS halt_group
    FROM halt_points
    WINDOW w AS (PARTITION BY jrnid ORDER BY tst)
  ),
  halt_groups AS (
    SELECT
      jrnid,
      halt_group,
      min(tst)                                                      AS tst,
      extract(epoch FROM max(end_tst) - min(tst))                   AS total_s,
      coalesce(sum(represents_time_s) FILTER(WHERE drst), 0.0)      AS door_open_s,
      coalesce(sum(represents_time_s) FILTER(WHERE NOT drst), 0.0)  AS door_closed_s,
      sum(represents_time_s)                                        AS represents_time_s
    FROM grouped_halt_points
    GROUP BY jrnid, halt_group
  )
  SELECT jrnid, tst, total_s, door_open_s, door_closed_s, represents_time_s
  FROM halt_groups
  WHERE total_s >= $1;
$$;

CREATE PROCEDURE obs.batch_create_halts_on_journey(
  min_halt_duration_s float4 DEFAULT 3.0
) LANGUAGE PLPGSQL
AS $$
BEGIN
  INSERT INTO obs.halt_on_journey
  SELECT *
  FROM obs.get_halts_on_journey(min_halt_duration_s)
  ORDER BY jrnid, tst;
END;
$$;
