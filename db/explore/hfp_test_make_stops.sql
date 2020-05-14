CREATE TABLE test_jrn.stops AS (
  WITH sts AS (
    SELECT unnest(stop_ids) AS stopid
    FROM stage_gtfs.trip_template_arrays
    WHERE ttid = '1007_1_21'
  )
  SELECT
    sts.stopid,
    row_number() OVER () AS rn,
    nd.nodeid,
    nd.geom
  FROM sts
  INNER JOIN nw.stops AS s
  ON sts.stopid = s.stopid
  INNER JOIN nw.nodes AS nd
  ON s.nodeid = nd.nodeid
);
