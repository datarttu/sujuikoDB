/*
 * In the tram network, there are multiple cases in the GTFS
 * where a stop has significant offset from its real location and it is
 * then snapped to an OSM link with wrong direction.
 * This table should show most of these cases.
 * However, note that we are using contracted network with some very long links,
 * so some of these stops are still correct, for example near termini
 * where end loops end up being merged into a single long link.
 * So you should make the corrections interactively anyway (e.g. in QGIS).
 * Do NOT run this file as a whole!
 *
 * Arttu K 2020-03
 */

BEGIN;

CREATE TABLE stage_nw.missnapped_tram_stops AS (
  WITH snapped_tram_stops AS (
    SELECT s.stopid, g.name, s.edgeid, s.edge_start_dist, s.geom
    FROM stage_nw.snapped_stops           AS s
    INNER JOIN stage_gtfs.stops_with_mode AS g
    ON s.stopid = g.stopid
    WHERE g.mode = 'tram'::mode_type
  )
  SELECT
    s1.stopid AS stopid_l, s2.stopid AS stopid_r,
    s1.name AS name_l, s2.name AS name_r,
    s1.edgeid AS edgeid,
    abs(s1.edge_start_dist - s2.edge_start_dist) AS dist_diff,
    s1.geom,
    'original'::text AS position_status
  FROM snapped_tram_stops AS s1
  INNER JOIN LATERAL (
    SELECT o.stopid, o.name, o.edgeid, o.edge_start_dist
    FROM snapped_tram_stops AS o
    WHERE s1.edgeid = o.edgeid
      AND s1.stopid <> o.stopid
      AND s1.name = o.name
    ORDER BY abs(s1.edge_start_dist - o.edge_start_dist)
    LIMIT 1
  ) AS s2
  ON true
  ORDER BY dist_diff ASC
);

ALTER TABLE stage_nw.missnapped_tram_stops
ADD PRIMARY KEY (stopid_l);

CREATE FUNCTION stage_nw.update_missnapped_tram_stops_status()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
  NEW.position_status = 'moved';
  RETURN NEW;
END;
$$;

CREATE TRIGGER mark_fixed_tram_stop_points
BEFORE UPDATE OF geom ON stage_nw.missnapped_tram_stops
FOR EACH ROW
WHEN (NOT ST_Equals(OLD.geom, NEW.geom))
EXECUTE FUNCTION stage_nw.update_missnapped_tram_stops_status();

COMMIT;

/*
 * Do modifications interactively at this point!
 */

BEGIN;
UPDATE stage_gtfs.stops_with_mode
SET geom = m.geom
FROM stage_nw.missnapped_tram_stops AS m
WHERE stopid = m.stopid_l
  AND m.position_status = 'moved';
COMMIT;

/*
 * Fixed positions as of 10.3.2020:
 */
COPY (
  SELECT
    stopid,
    --name,
    geom
    --stop_lon AS lon_old,
    --stop_lat AS lat_old,
    --ST_X(ST_Transform(geom, 4326)) AS lon_new,
    --ST_Y(ST_Transform(geom, 4326)) AS lat_new
  FROM stage_gtfs.stops_with_mode AS a
  INNER JOIN stage_gtfs.stops     AS b
  ON a.stopid = b.stop_id
  WHERE stopid IN (
    1070421,
    1080405,
    1010424,
    1020450,
    1020458,
    1080404,
    1020443,
    1201430,
    1203415,
    1130437,
    1130443,
    1140449,
    1173403,
    1180437,
    1180439,
    1180441,
    1220426,
    1220431,
    1080410,
    1080415,
    1090416,
    1203405,
    1203406,
    1240403,
    1240418,
    1301450,
    1301452
  )
) TO STDOUT CSV HEADER;
