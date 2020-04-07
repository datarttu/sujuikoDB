/*
 * Create tables and functions for network preparing schema.
 *
 * Arttu K 2020-03
 */
\set ON_ERROR_STOP on
\c sujuiko;

BEGIN;

DROP SCHEMA IF EXISTS stage_nw CASCADE;
CREATE SCHEMA stage_nw;

CREATE TABLE stage_nw.raw_nw (
  id            bigint                      PRIMARY KEY,
  source        bigint                          NULL,
  target        bigint                          NULL,
  cost          double precision            DEFAULT -1,
  reverse_cost  double precision            DEFAULT -1,
  oneway        text                        NOT NULL,
  mode          public.mode_type            NOT NULL,
  geom          geometry(LINESTRING, 3067)  NOT NULL
);

CREATE INDEX raw_nw_geom_idx
  ON stage_nw.raw_nw
  USING GIST (geom);
CREATE INDEX raw_nw_source_idx
  ON stage_nw.raw_nw (source);
CREATE INDEX raw_nw_target_idx
  ON stage_nw.raw_nw (target);

CREATE OR REPLACE FUNCTION stage_nw.populate_raw_nw()
RETURNS TABLE (
  by_mode         public.mode_type,
  rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  IF EXISTS (SELECT * FROM stage_nw.raw_nw LIMIT 1)
  THEN
    RAISE EXCEPTION 'Table stage_nw.raw_nw is not empty!'
    USING HINT = 'Truncate the table first.';
  END IF;

  RAISE NOTICE 'Populating stage_nw.raw_nw ...';
  INSERT INTO stage_nw.raw_nw (
    id, cost, reverse_cost, oneway, mode, geom
  )
  SELECT
    c.osm_id AS id,
    ST_Length(c.geom) AS cost,
    CASE
      WHEN c.oneway LIKE 'FT' THEN -1
      ELSE ST_Length(c.geom)
      END AS reverse_cost,
    c.oneway,
    c.mode,
    c.geom
  FROM stage_osm.combined_lines AS c;

  RAISE NOTICE 'Creating pgr topology on stage_nw.raw_nw ...';
  -- NOTE: tolerance in meters! (EPSG 3067)
  PERFORM pgr_createTopology(
    'stage_nw.raw_nw',
    1.0,
    the_geom := 'geom',
    id := 'id',
    source := 'source',
    target := 'target',
    rows_where := 'true',
    clean := true
  );

  RETURN QUERY
  SELECT
    mode AS by_mode,
    count(mode) AS rows_inserted
  FROM stage_nw.raw_nw
  GROUP BY mode;
END;
$$;
COMMENT ON FUNCTION stage_nw.populate_raw_nw IS
'Read line geometries from stage_osm.combined_lines
into stage_nw.raw_nw and create pgr routing topology.';

CREATE OR REPLACE FUNCTION stage_nw.analyze_inout_edges()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE cnt integer;
BEGIN
  RAISE NOTICE 'Counting incoming and outgoing oneway edges ...';

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS owein integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET owein = results.cnt
    FROM (
      SELECT target AS id, count(target) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'FT'
      GROUP BY target
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"owein" set for % rows', cnt;

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS oweout integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET oweout = results.cnt
    FROM (
      SELECT source AS id, count(source) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'FT'
      GROUP BY source
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"oweout" set for % rows', cnt;

  RAISE NOTICE 'Counting incoming and outgoing two-way edges ...';

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS twein integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET twein = results.cnt
    FROM (
      SELECT target AS id, count(target) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'B'
      GROUP BY target
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"twein" set for % rows', cnt;

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS tweout integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET tweout = results.cnt
    FROM (
      SELECT source AS id, count(source) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'B'
      GROUP BY source
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"tweout" set for % rows', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.analyze_inout_edges IS
'For each vertex in stage_nw.raw_nw_vertices_pgr,
calculate number of incoming and outgoing oneway
and two-way edges.
Adds integer columns "owein", "oweout", "twein" and "tweout".';

CREATE OR REPLACE FUNCTION stage_nw.build_contracted_network()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE forbidden integer[];
BEGIN
  RAISE NOTICE 'Checking restricted vertices ...';
  EXECUTE '
  SELECT array_agg(id)
  FROM stage_nw.raw_nw_vertices_pgr
  WHERE NOT (
    (owein = 1 and oweout = 1 and twein = 0 and tweout = 0)
    OR
    (owein = 0 and oweout = 0 and twein = 1 and tweout = 1)
    OR
    (owein = 0 and oweout = 0 and twein = 2 and tweout = 0)
    OR
    (owein = 0 and oweout = 0 and twein = 0 and tweout = 2)
  );'
  INTO forbidden;

  RAISE NOTICE 'Building contracted network from stage_nw.raw_nw ...';

  RAISE NOTICE '  Creating table for contracted vertices arrays ...';
  DROP TABLE IF EXISTS stage_nw.contracted_arr;
  CREATE TABLE stage_nw.contracted_arr AS (
    SELECT id, contracted_vertices, source, target, cost
    FROM pgr_contraction(
      'SELECT id, source, target, cost, reverse_cost
       FROM stage_nw.raw_nw',
       ARRAY[2], -- _linear_ contraction (2), as opposed to dead-end (1)
       max_cycles := 1,
       forbidden_vertices := forbidden,
       directed := true
    )
  );

  RAISE NOTICE '  Creating table for contracted edges to merge ...';
  DROP TABLE IF EXISTS stage_nw.contracted_edges_to_merge;
  CREATE TABLE stage_nw.contracted_edges_to_merge AS (
    WITH
      /*
       * Two-way links are represented twice in contraction arrays,
       * therefore we only pick distinct ones.
       */
      distinct_contraction_arrays AS (
        SELECT DISTINCT ON (contracted_vertices) *
        FROM stage_nw.contracted_arr
      ),
      /*
       * Open up intermediate vertices ("via points") to rows.
       */
      unnested AS (
        SELECT
          id AS grp,
          source::bigint,
          target::bigint,
          unnest(contracted_vertices)::bigint AS vertex
        FROM distinct_contraction_arrays
      ),
      /*
       * From possible combos of source-intermediate, intermediate-intermediate
       * and intermediate-end vertice ids, we find the ones that actually
       * have a matching link on the network, and then we can assign a
       * contraction group id to them.
       * We do this because the contraction algorithm did not output the
       * vertex array in the same order they appear on the links to merge.
       */
      vertex_pair_candidates AS (
        SELECT grp, source AS source, vertex AS target
        FROM unnested
        UNION
        SELECT grp, vertex AS source, target AS target
        FROM unnested
        UNION
        SELECT grp, target AS source, vertex AS target
        FROM unnested
        UNION
        SELECT grp, vertex AS source, source AS target
        FROM unnested
        UNION
        SELECT u1.grp AS grp, u1.vertex AS source, u2.vertex AS target
        FROM unnested       AS u1
        INNER JOIN unnested AS u2
          ON (u1.grp = u2.grp AND u1.vertex <> u2.vertex)
      )
    SELECT n.id, vpc.grp, n.source, n.target
    FROM stage_nw.raw_nw AS n
    INNER JOIN vertex_pair_candidates AS vpc
    ON n.source = vpc.source AND n.target = vpc.target
    ORDER BY vpc.grp, n.id
  );

  RAISE NOTICE '  Creating contracted network edge table ...';
  DROP TABLE IF EXISTS stage_nw.contracted_nw;
  CREATE TABLE stage_nw.contracted_nw AS (
    WITH
      all_edges_before_merging AS (
        SELECT
          raw.id,
          raw.source,
          raw.target,
          raw.oneway,
          raw.mode,
          coalesce(ctr.grp, raw.id) AS merge_group,
          raw.geom
        FROM stage_nw.raw_nw AS raw
        LEFT JOIN stage_nw.contracted_edges_to_merge AS ctr
        ON raw.source = ctr.source AND raw.target = ctr.target
      )
    SELECT
      merge_group::bigint             AS id,
      NULL::bigint                    AS source,
      NULL::bigint                    AS target,
      min(oneway)                     AS oneway,
      min(mode)                       AS mode,
      ST_LineMerge(ST_Collect(geom))  AS geom,
      false                           AS is_contracted
    FROM all_edges_before_merging
    GROUP BY merge_group
  );

  RAISE NOTICE '  Updating contracted network edge ids ...';
  /*
   * Replace negative ids produced by the contraction routine.
   * We rely on the fact that the least id used by OSM-based edges
   * is > 2,000,000, and there are just thousands of contracted edges,
   * so we just flip the negative id sign.
   */
  UPDATE stage_nw.contracted_nw
  SET
    id = abs(id),
    is_contracted = true
  WHERE id < 0;

  RAISE NOTICE '  Adding primary key on contracted network ...';
  ALTER TABLE stage_nw.contracted_nw
  ADD PRIMARY KEY (id);

  RAISE NOTICE '  Creating pgr topology on contracted network ...';
  PERFORM pgr_createTopology(
    'stage_nw.contracted_nw',
    1.0,
    the_geom := 'geom',
    id := 'id',
    source := 'source',
    target := 'target',
    rows_where := 'true',
    clean := true
  );

  RETURN 'OK';

END;
$$;
COMMENT ON FUNCTION stage_nw.build_contracted_network IS
'Creates a new network from stage_nw.raw_nw where linear edges
(i.e., continuous one- or two-way edge groups between intersections)
are merged, resulting in fewer edges in total.
Detecting vertices restricted from contraction
requires that stage_nw.analyze_inout_edges() is run first.
Creates new network tables stage_nw.contracted_nw
and stage_nw.contracted_nw_vertices_pgr.';

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

CREATE OR REPLACE FUNCTION stage_nw.populate_nw_links()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM nw.links;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from nw.links', cnt;

  WITH
    /*
     * The stops table we use has already a value
     * that tells us how far away the stop point is located from the edge start
     * (along the edge). We use this to split the edges at stop locations.
     */
    distances_ordered AS (
      SELECT DISTINCT ON (edges.id, stops.edge_start_dist)
        edges.id              AS edge,
        edges.geom            AS geom,
        ST_Length(edges.geom) AS len,
        edges.mode            AS mode,
        edges.oneway          AS oneway,
        stops.edge_start_dist AS dist
      FROM stage_nw.contracted_nw       AS edges
      INNER JOIN stage_nw.snapped_stops AS stops
        ON edges.id = stops.edgeid
      WHERE stops.edge_start_dist > 0
        AND stops.edge_start_dist < ST_Length(edges.geom)
      ORDER BY stops.edge_start_dist ASC
    ),
    /*
     * An UNION is required since the first data set here will not include
     * the last part of the split edge;
     * we construct the last parts separately.
     * GROUP BY is required because we have snapped stops close to each other
     * to the same location, so without grouping, this would result in
     * same split edge occurring multiple times.
     */
    splits AS (
      SELECT
        edge,
        mode,
        oneway,
        geom,
        coalesce(
          lag(dist) OVER (PARTITION BY edge ORDER BY dist),
          0) / len  AS start_frac,
        dist / len  AS end_frac
      FROM distances_ordered
      UNION
      SELECT
        edge,
        min(mode)   AS mode,
        min(oneway) AS oneway,
        geom,
        max(dist) / max(len) AS start_frac,
        1 AS end_frac
      FROM distances_ordered
      GROUP BY edge, geom
    ),
    /*
     * Note that the above only included those edges that are somehow related
     * to one or more stops. We want to include the rest of the edges too,
     * i.e. the not split ones.
     */
    combined AS (
    SELECT
      edge,
      mode,
      oneway,
      /*
       * Store these distance fraction values later
       * if needed for diagnostics or debugging.
       * For now, we do not save them for production.
      start_frac,
      end_frac,
       */
      ST_LineSubstring(geom, start_frac, end_frac) AS geom
    FROM splits
    UNION
    SELECT
      edges.id      AS edge,
      edges.mode    AS mode,
      edges.oneway  AS oneway,
      /*
      0::real       AS start_frac,
      1::real       AS end_frac,
      */
      edges.geom    AS geom
    FROM stage_nw.contracted_nw   AS edges
    WHERE edges.id NOT IN
      (
        SELECT DISTINCT edge
        FROM splits
      )
    )
  /*
   * At this point, we lose the previous edge id information (for now at least)
   * and use a running link id instead.
   * However, split links with the same original edge id should have
   * consecutive ids.
   * Moreover, we move from the 'B-FT' oneway marking system, used by the
   * contraction algorithm, to cost-rcost system used by routing algorithms.
   */
  INSERT INTO nw.links (linkid, mode, cost, rcost, geom, wgs_geom)
  (
    SELECT
      row_number() OVER (ORDER BY edge) AS linkid,
      mode,
      ST_Length(geom)                   AS cost,
      CASE
        WHEN oneway = 'B' THEN ST_Length(geom)
        ELSE -1
      END                               AS rcost,
      geom,
      ST_Transform(geom, 4326)          AS wgs_geom
    FROM combined
  );
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into nw.links', cnt;
  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.populate_nw_links IS
'Split contracted network edges by stop locations,
and insert the resulting network edges to nw.links table.
nw.links will be emptied first.
Requires populated stage_nw.contracted_nw
and stage_nw.snapped_stops tables.';

CREATE OR REPLACE FUNCTION stage_nw.populate_nw_stops()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  IF NOT EXISTS (SELECT * FROM nw.nodes LIMIT 1) THEN
    RAISE EXCEPTION 'nw.nodes is empty!'
    USING HINT = 'Run nw.create_node_table first.';
  END IF;

  DELETE FROM nw.stops;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from nw.stops', cnt;

  INSERT INTO nw.stops (
    stopid, nodeid, mode, code, name, descr, parent
  ) (
    SELECT
      a.stopid,
      c.nodeid,
      a.mode,
      a.code,
      a.name,
      a.descr,
      a.parent
    FROM stage_gtfs.stops_with_mode   AS a
    INNER JOIN stage_nw.snapped_stops AS b
      ON a.stopid = b.stopid
    INNER JOIN nw.nodes               AS c
      ON ST_DWithin(b.geom, c.geom, 0.01)
    ORDER BY c.nodeid, a.stopid
  );
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into nw.stops', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.populate_nw_stops IS
'Fill nw.stops stopid-nodeid table
by matching stage_nw.snapped_stops
and additional attributes from GTFS stops
with nw.nodes point locations.
nw.stops will be emptied first.
nw.nodes must be correctly created first.';

COMMIT;
