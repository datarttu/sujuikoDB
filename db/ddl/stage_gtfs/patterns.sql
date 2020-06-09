CREATE TABLE stage_gtfs.patterns (
  ptid              text          PRIMARY KEY,
  route             text          NOT NULL REFERENCES sched.routes(route),
  dir               smallint      NOT NULL CHECK (dir IN (1, 2)),
  shape_id          text          NOT NULL REFERENCES stage_gtfs.shape_lines(shape_id),
  trip_ids          text[],
  invalid_reasons   text[]        DEFAULT '{}'
);
CREATE INDEX ON stage_gtfs.patterns USING GIN(trip_ids);
COMMENT ON TABLE stage_gtfs.patterns IS
'Staging table for `sched.patterns`.
Each `ptid` represents a variant of `route & dir` consisting of a unique sequence of stops.
It is also possible to have two or more identical sets of stops but different `shape_id` values,
e.g. in case of a diverted itinerary variant that differs only between a stop pair while not affecting the stops served.
- `shape_id`: corresponding GTFS shape id
- `trip_ids`: all the GTFS trip ids that were grouped together to form the pattern
- `invalid_reasons`: reasons to invalidate a record can be gathered here, e.g.
  no complete network path exists, or the network path differs too much from the GTFS shape';

CREATE TABLE stage_gtfs.pattern_stops (
  ptid              text                        NOT NULL REFERENCES stage_gtfs.patterns(ptid),
  stop_seq          smallint                    NOT NULL,
  ij_stops          integer[]                   NOT NULL CHECK (cardinality(ij_stops) = 2),
  ij_nodes          integer[],
  ij_shape_dists    double precision[],
  restricted_links  integer[],
  path_found        boolean                     DEFAULT false,
  shape_geom        geometry(LINESTRING, 3067),
  max_offset        double precision,
  invalid_reasons   text[]                      DEFAULT '{}',

  PRIMARY KEY (ptid, stop_seq)
);
CREATE INDEX ON stage_gtfs.pattern_stops USING GIN(ij_stops);
CREATE INDEX ON stage_gtfs.pattern_stops USING GIN(ij_nodes);
CREATE INDEX ON stage_gtfs.pattern_stops USING GIST(shape_geom);
COMMENT ON TABLE stage_gtfs.pattern_stops IS
'Describes the stop sequences of patterns `ptid`, belonging to `stage_gtfs.patterns`,
as stop-to-stop pairs `ij_stops` ordered by `stop_seq`.
- `ij_nodes`: nodes on the network corresponding to the stop ids
- `ij_shape_dists`: relative distances along the GTFS shape,
  used to extract the linestring subsection that corresponds to the stop pair
- `restricted_links`: can be populated manually, before finding the shortest paths,
  to restrict certain link/edge ids from being used for routing
- `path_found`: flag after finding the shortest paths - does a path exist for this pair?
- `shape_geom`: corresponding subsection of the GTFS shape
- `max_offset`: maximum distance between `shape_geom` and the network path,
  if this is too high then the network path differs too much from the designed path in GTFS
- `invalid_reasons`: reasons to invalidate a record can be gathered here, e.g.
  no network path exists, or the network path differs too much from the GTFS shape';

CREATE TABLE stage_gtfs.stop_pairs (
  ij_stops          integer[]   PRIMARY KEY,
  ij_nodes          integer[],
  ptids             text[],
  n_patterns        integer,
  path_found        boolean     DEFAULT false
);
COMMENT ON TABLE stage_gtfs.stop_pairs IS
'Unique pairs of stops, as two-element arrays, that occur successively on any stop pattern
in `stage_gtfs.pattern_stops`.
- `ij_nodes`: nw.nodes node ids corresponding to the stops, used as start and end vertices for routing
- `ptids`: which patterns `ptid` do use the stop pair?
- `n_patterns`: how many patterns in `.pattern_stops` do use the stop pair?
  (Effectively the same as `ptids` length).
- `path_found`: has a network path between the stops been found?';

CREATE FUNCTION stage_gtfs.extract_trip_stop_patterns(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RAISE NOTICE 'Extracting trip stop patterns to stage_gtfs.patterns ...';
  RETURN QUERY
  WITH
    arrays_by_trip_id AS (
      SELECT
        trip_id,
        array_agg(stop_id ORDER BY stop_sequence)           AS stopids,
        array_agg(rel_dist_traveled ORDER BY stop_sequence) AS rel_distances,
        array_agg(stop_sequence)                            AS stop_seqs
      FROM stage_gtfs.normalized_stop_times
      GROUP BY trip_id
    ),
    records_by_patterns AS (
      SELECT
        twd.route_id            AS route,
        twd.direction_id        AS dir,
        twd.shape_id            AS shape_id,
        arr.stopids             AS stopids,
        /* NOTE: We use (arbitrarily) min() here to get exactly one array record for each group.
         *       Should there be more of different rel_distances or stop_seqs
         *       variants per route-dir-shape_id-stopids,
         *       the rest of them are discarded now.
         */
        min(arr.rel_distances)  AS rel_distances,
        min(arr.stop_seqs)      AS stop_seqs,
        array_agg(arr.trip_id ORDER BY arr.trip_id) AS trip_ids
      FROM arrays_by_trip_id                  AS arr
      INNER JOIN stage_gtfs.trips_with_dates  AS twd
        ON arr.trip_id = twd.trip_id
      GROUP BY route, dir, shape_id, stopids
      ORDER BY route, dir
    ),
    patterns_with_ptid_and_seq AS (
      SELECT
        concat_ws(
          '_',
          route, dir,
          row_number() OVER (PARTITION BY route, dir)
        ) AS ptid,
        route, dir, trip_ids, stopids, shape_id, rel_distances, stop_seqs
      FROM records_by_patterns
    ),
    insert_patterns AS (
      INSERT INTO stage_gtfs.patterns (
        ptid, route, dir, shape_id, trip_ids
      )
      SELECT ptid, route, dir, shape_id, trip_ids
      FROM patterns_with_ptid_and_seq
      RETURNING *
    ),
    open_patterns AS (
      SELECT
        ptid,
        unnest(stopids)                       AS stopid,
        unnest(rel_distances)                 AS rel_dist,
        unnest(stop_seqs)                     AS stop_seq
      FROM patterns_with_ptid_and_seq
    ),
    stop_pairs AS (
      SELECT
        ptid,
        stop_seq,
        ARRAY[stopid, lead(stopid) OVER w_ptid]::integer[]                AS ij_stops,
        ARRAY[rel_dist, lead(rel_dist) OVER w_ptid]::double precision[]   AS ij_shape_dists
      FROM open_patterns
      WINDOW w_ptid AS (PARTITION BY ptid ORDER BY stop_seq)
    ),
    insert_stops AS (
      INSERT INTO stage_gtfs.pattern_stops (
        ptid, stop_seq, ij_stops, ij_shape_dists
      )
      SELECT ptid, stop_seq, ij_stops, ij_shape_dists
      FROM stop_pairs
      WHERE ij_stops[2] IS NOT NULL
      ORDER BY ptid, stop_seq
      RETURNING *
    )
    SELECT 'stage_gtfs.patterns' AS table_name, count(*) AS rows_affected
    FROM insert_patterns
    UNION
    SELECT 'stage_gtfs.pattern_stops' AS table_name, count(*) AS rows_affected
    FROM insert_stops;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.extract_trip_stop_patterns IS
'Populate `stage_gtfs.patterns` and `.pattern_stops` by extracting unique stop sequences
by route and direction from `stage_gtfs.normalized_stop_times`.
This will not check if the target tables are already populated,
but running this on non-empty tables will probably fail since `ptid` values are
always generated with running numbers from 1, which can lead to conflicts with existing values.
- `where_sql`: NOT IMPLEMENTED YET. Use this to filter the set of records read
  from `stage_gtfs.normalized_stop_times`.';


CREATE OR REPLACE FUNCTION stage_gtfs.extract_unique_stop_pairs()
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       bigint;
  cnt_nulls bigint;
BEGIN
  RAISE NOTICE 'Extracting unique stop pairs from stage_gtfs.pattern_stops ...';
  WITH
    unique_pairs AS (
      SELECT
        ij_stops,
        array_agg(ptid) AS ptids,
        count(*)        AS n_patterns
      FROM stage_gtfs.pattern_stops
      GROUP BY ij_stops
    ),
    inserted AS (
      INSERT INTO stage_gtfs.stop_pairs (
        ij_stops, ij_nodes, ptids, n_patterns
      )
      SELECT
        up.ij_stops,
        ARRAY[s1.nodeid, s2.nodeid]::integer[]  AS ij_nodes,
        up.ptids,
        up.n_patterns
      FROM unique_pairs   AS up
      LEFT JOIN nw.stops  AS s1
        ON s1.stopid = up.ij_stops[1]
      LEFT JOIN nw.stops  AS s2
        ON s2.stopid = up.ij_stops[2]
      RETURNING *
    )
  SELECT INTO cnt count(*) FROM inserted;

  SELECT INTO cnt_nulls count(*)
  FROM stage_gtfs.stop_pairs
  WHERE ij_nodes[1] IS NULL OR ij_nodes[2] IS NULL;
  IF cnt_nulls > 0 THEN
    RAISE WARNING '% rows where one or both node ids are NULL in stage_gtfs.stop_pairs', cnt_nulls;
  END IF;

  RETURN QUERY
  SELECT 'stage_gtfs.stop_pairs' AS table_name, cnt AS rows_affected;
END;
$$;
