CREATE TABLE stage_nw.snapped_stops (
  stopid            integer           PRIMARY KEY,
  edgeid            bigint            NOT NULL,
  point_dist        double precision,
  edge_start_dist   double precision,
  edge_end_dist     double precision,
  status            text,
  geom              geometry(POINT, 3067)
);
CREATE INDEX ON stage_nw.snapped_stops USING GIST(geom);

CREATE OR REPLACE FUNCTION stage_nw.snap_stops_to_network()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM stage_nw.snapped_stops CASCADE;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_nw.snapped stops', cnt;
  WITH projected AS (
    SELECT
      s.stopid::integer                   AS stopid,
      n.id::bigint                        AS edgeid,
      ST_Distance(s.geom, n.geom)         AS point_dist,
      ST_LineLocatePoint(n.geom, s.geom)  AS location_along,
      ST_Length(n.geom)                   AS edge_length,
      n.geom                              AS edge_geom
    FROM stage_gtfs.stops_with_mode AS s
    INNER JOIN LATERAL (
      SELECT e.id, e.geom
      FROM stage_nw.contracted_nw AS e
      WHERE e.mode = s.mode
      ORDER BY s.geom <-> e.geom
      LIMIT 1
    ) AS n
    ON true
  )
  INSERT INTO stage_nw.snapped_stops
  SELECT
    stopid::integer,
    edgeid::bigint,
    point_dist,
    location_along * edge_length        AS edge_start_dist,
    (1 - location_along) * edge_length  AS edge_end_dist,
    'snapped'::text                     AS status,
    ST_LineInterpolatePoint(
      edge_geom, location_along)        AS geom
  FROM projected;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'stage_nw.snapped_stops populated with % rows', cnt;
  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.snap_stops_to_network IS
'Populate stage_nw.snapped_stops with stop points projected
on the nearest network edge line geometries,
with distance to original point location, distances to edge start
and end along the edge, and projected point geometry.
Requires stop points from table stage_gtfs.stops_with_mode
and network edges from table stage_nw.contracted_nw.';

CREATE OR REPLACE FUNCTION stage_nw.delete_outlier_stops(
  tolerance     double precision    DEFAULT 20.0
)
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM stage_nw.snapped_stops
  WHERE point_dist > tolerance;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE
    'Deleted % stops from stage_nw.snapped_stops farther than % units away from edges',
    cnt, tolerance;
  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.delete_outlier_stops IS
'From stage_nw.snapped_stops,
delete the ones whose original distance from the edge
exceeds the tolerance in projection unit.';

CREATE OR REPLACE FUNCTION stage_nw.snap_stops_near_nodes(
  tolerance     double precision    DEFAULT 10.0
)
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  UPDATE stage_nw.snapped_stops AS s
  SET
    geom = ST_StartPoint(e.geom),
    edge_start_dist = 0,
    edge_end_dist = ST_Length(e.geom),
    status = 'moved to edge start'
  FROM stage_nw.contracted_nw AS e
  WHERE s.edgeid = e.id
    AND s.edge_start_dist > 0
    AND s.edge_start_dist < tolerance
    AND s.edge_start_dist <= s.edge_end_dist;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE
    '% stops closer than % units to link start moved to link start',
    cnt, tolerance;
  UPDATE stage_nw.snapped_stops AS s
  SET
    geom = ST_EndPoint(e.geom),
    status = 'moved to edge end',
    edge_start_dist = ST_Length(e.geom),
    edge_end_dist = 0
  FROM stage_nw.contracted_nw AS e
  WHERE s.edgeid = e.id
    AND s.edge_end_dist > 0
    AND s.edge_end_dist < tolerance
    AND s.edge_end_dist < s.edge_start_dist;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE
    '% stops closer than % units to link end moved to link end',
    cnt, tolerance;
  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.snap_stops_near_nodes IS
'Update stage_nw.snapped_stops point geoms using stage_nw.contracted_nw edges
such that any stop closer than `tolerance` to edge start point is snapped to
that point, or to end point respectively.
If distance to both start and end is less than the tolerance,
the closer one is preferred.';

CREATE OR REPLACE FUNCTION stage_nw.grouped_stops_on_edge(
  tolerance     double precision   DEFAULT 10.0
)
RETURNS TABLE (
  stopid              integer,
  edgeid              bigint,
  edge_start_dist     double precision,
  cluster_group       integer
)
LANGUAGE PLPGSQL
STABLE
AS $$
DECLARE
  rec             record;
  recout          record;
  current_edge    bigint    := 0;
  prev_distance   double precision;
BEGIN
  FOR rec IN
    SELECT s.stopid, s.edgeid, s.edge_start_dist
    FROM stage_nw.snapped_stops AS s
    ORDER BY s.edgeid, s.edge_start_dist
  LOOP
    IF rec.edgeid <> current_edge THEN
      current_edge    := rec.edgeid;
      cluster_group   := 0;
      prev_distance   := rec.edge_start_dist;
    ELSE
      IF rec.edge_start_dist > (prev_distance + tolerance) THEN
        cluster_group := cluster_group + 1;
        prev_distance := rec.edge_start_dist;
      END IF;
    END IF;
    stopid          := rec.stopid;
    edgeid          := rec.edgeid;
    edge_start_dist := rec.edge_start_dist;
    RETURN NEXT;
  END LOOP;
END;
$$;
COMMENT ON FUNCTION stage_nw.grouped_stops_on_edge IS
'Return stops from stage_nw.snapped_stops such that particular stops
get a common cluster group id if they are within `tolerance` meters
from each other along the edge. Grouping is started from the first
stop on the edge based on distance from edge start.';

CREATE OR REPLACE FUNCTION stage_nw.cluster_stops_on_edges(
  tolerance       double precision    DEFAULT 10.0
)
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  ALTER TABLE stage_nw.snapped_stops
    ADD COLUMN IF NOT EXISTS groupid integer;

  UPDATE stage_nw.snapped_stops AS s
  SET groupid = g.cluster_group
  FROM stage_nw.grouped_stops_on_edge(tolerance := tolerance) AS g
  WHERE s.stopid = g.stopid;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'Cluster groupid set for % stops', cnt;

  WITH
    /*
     * New location is determined by average distance of stops of the groups
     * from the edge start.
     */
    avg_distances AS (
      SELECT edgeid, groupid, avg(edge_start_dist) AS new_dist, count(*)
      FROM stage_nw.snapped_stops
      GROUP BY edgeid, groupid
      HAVING count(*) > 1
    )
  UPDATE stage_nw.snapped_stops AS s
  SET
    edge_start_dist = a.new_dist,
    edge_end_dist = ST_Length(e.geom) - a.new_dist,
    geom = ST_LineInterpolatePoint(e.geom, a.new_dist / ST_Length(e.geom)),
    status = 'grouped'
  FROM
    avg_distances AS a,
    stage_nw.contracted_nw AS e
  WHERE s.edgeid = a.edgeid
    AND s.groupid = a.groupid
    AND s.edgeid = e.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE
    'New edge_start_dist, edge_end_dist, geom and status set for % stops',
    cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.cluster_stops_on_edges IS
'Update stage_nw.snapped_stops by grouping stops located close to each other
on the same edge to the same point location within group.
Adds a new column "groupid" if not already present.';
