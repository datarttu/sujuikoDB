DROP TABLE IF EXISTS stage_gtfs.patterns CASCADE;
CREATE TABLE stage_gtfs.patterns (
  ptid              text              PRIMARY KEY,
  route             text              NOT NULL REFERENCES sched.routes(route),
  dir               smallint          NOT NULL CHECK (dir IN (1, 2)),
  shape_id          text              NOT NULL REFERENCES stage_gtfs.shape_lines(shape_id),
  trip_ids          text[],
  shape_len_total   double precision,
  nw_len_total      double precision,
  nw_vs_shape_coeff double precision,
  invalid_reasons   text[]            DEFAULT '{}'
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

DROP TABLE IF EXISTS stage_gtfs.pattern_stops CASCADE;
CREATE TABLE stage_gtfs.pattern_stops (
  ptid              text                        NOT NULL REFERENCES stage_gtfs.patterns(ptid),
  stop_seq          smallint                    NOT NULL,
  ij_stops          integer[]                   NOT NULL CHECK (cardinality(ij_stops) = 2),
  ij_shape_dists    double precision[],
  restricted_links  integer[],
  path_found        boolean                     DEFAULT false,
  shape_geom        geometry(LINESTRING, 3067),
  nw_vs_shape_coeff double precision,
  invalid_reasons   text[]                      DEFAULT '{}',

  PRIMARY KEY (ptid, stop_seq)
);
CREATE INDEX ON stage_gtfs.pattern_stops USING GIN(ij_stops);
CREATE INDEX ON stage_gtfs.pattern_stops USING GIST(shape_geom);
COMMENT ON TABLE stage_gtfs.pattern_stops IS
'Describes the stop sequences of patterns `ptid`, belonging to `stage_gtfs.patterns`,
as stop-to-stop pairs `ij_stops` ordered by `stop_seq`.
- `ij_shape_dists`: relative distances along the GTFS shape,
  used to extract the linestring subsection that corresponds to the stop pair
- `restricted_links`: can be populated manually, before finding the shortest paths,
  to restrict certain link/edge ids from being used for routing
- `path_found`: flag after finding the shortest paths - does a path exist for this pair?
- `shape_geom`: corresponding subsection of the GTFS shape
- `nw_vs_shape_coeff`: `network path length / GTFS shape length`. If this differs considerably
  from `1.0`, then the network path is likely to be incorrect and should be fixed.
- `invalid_reasons`: reasons to invalidate a record can be gathered here, e.g.
  no network path exists, or the network path differs too much from the GTFS shape';

DROP TABLE IF EXISTS stage_gtfs.pattern_paths CASCADE;
CREATE TABLE stage_gtfs.pattern_paths (
  ptid              text        NOT NULL,
  stop_seq          smallint    NOT NULL,
  path_seq          integer     NOT NULL,
  linkid            integer     NOT NULL,
  seg_nodes         integer[]   NOT NULL,
  reversed          boolean     NOT NULL,

  PRIMARY KEY (ptid, stop_seq, path_seq),
  FOREIGN KEY (ptid, stop_seq) REFERENCES stage_gtfs.pattern_stops(ptid, stop_seq)
);
COMMENT ON TABLE stage_gtfs.pattern_paths IS
'Segments comprising the network paths between stop node pairs `stop_seq` belonging to pattern `ptid`.
Note that paths not found are not included here, so there can be "holes" in `stop_seq` values.
If `reversed` is true, then the segment uses the link `linkid` in the opposite direction,
i.e. start and end nodes are flipped (`seg_nodes`).';

DROP VIEW IF EXISTS stage_gtfs.view_pattern_paths_geom CASCADE;
CREATE OR REPLACE VIEW stage_gtfs.view_pattern_paths_geom AS (
  SELECT
    pp.*,
    CASE
      WHEN reversed IS true THEN li.geom
      ELSE ST_Reverse(li.geom)
    END AS geom
  FROM stage_gtfs.pattern_paths AS pp
  LEFT JOIN nw.links            AS li
    ON pp.linkid = li.linkid
);

DROP VIEW IF EXISTS stage_gtfs.view_pattern_stops_geom CASCADE;
CREATE OR REPLACE VIEW stage_gtfs.view_pattern_stops_geom AS (
  SELECT
    ps.*,
    ST_LineMerge( ST_Collect(vppg.geom) ) AS geom
  FROM stage_gtfs.pattern_stops                 AS ps
  LEFT JOIN stage_gtfs.view_pattern_paths_geom  AS vppg
    ON  ps.ptid = vppg.ptid
    AND ps.stop_seq = vppg.stop_seq
  GROUP BY ps.ptid, ps.stop_seq
);

DROP TABLE IF EXISTS stage_gtfs.stop_pairs CASCADE;
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

DROP TABLE IF EXISTS stage_gtfs.stop_pair_paths CASCADE;
CREATE TABLE stage_gtfs.stop_pair_paths (
  inode       integer     NOT NULL,
  jnode       integer     NOT NULL,
  path_seq    integer     NOT NULL,
  nodeid      integer     NOT NULL,
  linkid      integer,
  PRIMARY KEY (inode, jnode, path_seq)
);

DROP FUNCTION IF EXISTS stage_gtfs.extract_trip_stop_patterns(text);
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

DROP FUNCTION IF EXISTS stage_gtfs.extract_unique_stop_pairs;
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

DROP FUNCTION IF EXISTS stage_gtfs.find_stop_pair_paths(text);
CREATE OR REPLACE FUNCTION stage_gtfs.find_stop_pair_paths(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       integer;
  n_routed  integer;
  n_total   integer;
  n_otm     integer;
  esql      text;
  route_rec record;
BEGIN
  -- TODO: Implement where_sql to enable routing only a subset of pairs

  SELECT INTO n_total count(*) FROM stage_gtfs.stop_pairs;
  SELECT INTO n_otm count(DISTINCT ij_nodes[1])
  FROM stage_gtfs.stop_pairs
  WHERE ij_nodes[1] IS NOT NULL AND ij_nodes[2] IS NOT NULL;
  RAISE NOTICE 'Routing % unique node pairs as % one-to-many records ...', n_total, n_otm;

  -- "edges_sql" query used as pgr_Dijkstra function input.
  esql := '
  SELECT
    linkid  AS id,
    inode   AS source,
    jnode   AS target,
    cost,
    rcost   AS reverse_cost
  FROM nw.links';

  n_routed := 0;

  FOR route_rec IN
    WITH open_pairs AS (
      SELECT
        ij_nodes[1]   AS inode,
        ij_nodes[2]   AS jnode
      FROM stage_gtfs.stop_pairs
      WHERE ij_nodes[1] IS NOT NULL AND ij_nodes[2] IS NOT NULL
    )
    SELECT
      inode,
      array_agg(jnode ORDER BY jnode)  AS jnodes
    FROM open_pairs
    GROUP BY inode
    ORDER BY inode
  LOOP
    n_routed := n_routed + 1;
    IF n_routed % 1000 = 0 OR n_routed = n_otm THEN
      RAISE NOTICE '%/% one-to-many node pairs processed ...', n_routed, n_otm;
    END IF;
    INSERT INTO stage_gtfs.stop_pair_paths (
      inode, jnode, path_seq, nodeid, linkid
    )
    SELECT
      route_rec.inode   AS inode,
      end_vid           AS jnode,
      path_seq,
      node              AS nodeid,
      edge              AS linkid
    FROM pgr_Dijkstra(
      esql,
      route_rec.inode,
      route_rec.jnodes
    );
  END LOOP;

  RAISE NOTICE 'All node pairs processed';

  UPDATE stage_gtfs.stop_pairs AS upd
  SET path_found = true
  FROM (
    SELECT DISTINCT inode, jnode
    FROM stage_gtfs.stop_pair_paths
  ) AS spp
  WHERE upd.ij_nodes[1] = spp.inode
    AND upd.ij_nodes[2] = spp.jnode;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'Routes found for %/% successive node pairs', cnt, n_total;

  RETURN QUERY
  SELECT 'stage_gtfs.stop_pairs' AS table_name, cnt AS rows_affected
  UNION
  SELECT 'stage_gtfs.stop_pair_paths' AS table_name, count(*) AS rows_affected
  FROM stage_gtfs.stop_pair_paths;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.find_stop_pair_paths IS
'Find a path sequence along nw.links
for each non-null stop node pair in `stage_gtfs.stop_pairs`, using pgr_Dijkstra.
Store results in `stage_gtfs.stop_pair_paths`.';

DROP FUNCTION IF EXISTS stage_gtfs.set_pattern_paths(text);
CREATE OR REPLACE FUNCTION stage_gtfs.set_pattern_paths(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt_path_records  bigint;
  cnt_pair_paths    bigint;
  cnt_full_paths    bigint;
BEGIN
  RAISE NOTICE 'Setting pattern stop paths in stage_gtfs.pattern_paths ...';

  -- TODO: Implement where_sql

  WITH stop_pair_segments AS (
    SELECT
      ARRAY[inode, jnode]::integer[]                      AS stop_nodes,
      path_seq,
      linkid,
      ARRAY[nodeid, lead(nodeid) OVER w_pair]::integer[]  AS seg_nodes
    FROM stage_gtfs.stop_pair_paths
    WINDOW w_pair AS (PARTITION BY inode, jnode ORDER BY path_seq)
    ORDER BY inode, jnode, path_seq
  )
  INSERT INTO stage_gtfs.pattern_paths (
    ptid, stop_seq, path_seq, linkid, seg_nodes, reversed
  )
  SELECT
    ps.ptid,
    ps.stop_seq,
    sg.path_seq,
    sg.linkid,
    sg.seg_nodes,
    (sg.seg_nodes[2] = li.inode AND sg.seg_nodes[1] = li.jnode) AS reversed
  FROM stage_gtfs.pattern_stops     AS ps
  INNER JOIN stage_gtfs.stop_pairs  AS sp
    ON ps.ij_stops = sp.ij_stops
  INNER JOIN stop_pair_segments      AS sg
    ON sp.ij_nodes = sg.stop_nodes
  INNER JOIN nw.links                AS li
    ON sg.linkid = li.linkid
  WHERE sg.linkid > -1
  ORDER BY ps.ptid, ps.stop_seq, sg.path_seq;

  GET DIAGNOSTICS cnt_path_records = ROW_COUNT;

  -- TODO: Implement "manual" routing for pairs with restricted_links;
  --       UPDATE these records as they were already populated above
  --       to ensure consistency

  RETURN QUERY
  SELECT 'stage_gtfs.pattern_paths' AS table_name, cnt_path_records AS rows_affected;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.set_pattern_paths IS
'Find network path for each stop pair of pattern `ptid` in `stage_gtfs.pattern_stops`,
store results in `stage_gtfs.pattern_paths`.
1)  If there are no `restricted_links`, then look up the paths by node pairs from `.stop_pair_paths`.
2)  Otherwise, run pgr_Dijkstra for the pair in question with the `restricted_links` omitted from the network.
    NOT IMPLEMENTED YET.';

DROP FUNCTION IF EXISTS stage_gtfs.set_pattern_stops_shape_geoms(text);
CREATE OR REPLACE FUNCTION stage_gtfs.set_pattern_stops_shape_geoms(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RAISE NOTICE 'Setting GTFS shape subsections (shape_geom) in stage_gtfs.pattern_stops ...';
  -- TODO: Implement where_sql
  RETURN QUERY
  WITH
    subsections AS (
      SELECT
        ps.ptid,
        ps.stop_seq,
        ST_LineSubstring(sl.geom, ps.ij_shape_dists[1], ps.ij_shape_dists[2]) AS shape_geom
      FROM stage_gtfs.pattern_stops     AS ps
      INNER JOIN stage_gtfs.patterns    AS p
        ON ps.ptid = p.ptid
      INNER JOIN stage_gtfs.shape_lines AS sl
        ON p.shape_id = sl.shape_id
    ),
    updated AS (
      UPDATE stage_gtfs.pattern_stops AS upd
      SET shape_geom = ss.shape_geom
      FROM (
        SELECT * FROM subsections
      ) AS ss
      WHERE upd.ptid = ss.ptid AND upd.stop_seq = ss.stop_seq
      RETURNING *
    )
    SELECT 'stage_gtfs.pattern_stops' AS table_name, count(*) AS rows_affected
    FROM updated;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.set_pattern_stops_shape_geoms IS
'For each stop pair in `.pattern_stops`, set `shape_geom` by extracting the corresponding
subsection from the related GTFS shape from `.shape_lines`.
The subsection is interpolated linearly with `ij_shape_dists`.';

DROP FUNCTION IF EXISTS stage_gtfs.set_pattern_stops_path_found;
CREATE OR REPLACE FUNCTION stage_gtfs.set_pattern_stops_path_found()
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RAISE NOTICE 'Setting path_found flags in stage_gtfs.pattern_stops ...';
  RETURN QUERY
  WITH updated AS (
    UPDATE stage_gtfs.pattern_stops AS upd
    SET path_found = true
    FROM (
      SELECT DISTINCT ptid, stop_seq
      FROM stage_gtfs.pattern_paths
    ) AS pp
    WHERE upd.ptid = pp.ptid AND upd.stop_seq = pp.stop_seq
    RETURNING *
  )
  SELECT 'stage_gtfs.pattern_stops' AS table_name, count(*) AS rows_affected
  FROM updated;
END;
$$;

DROP FUNCTION IF EXISTS stage_gtfs.set_patterns_length_values;
CREATE OR REPLACE FUNCTION stage_gtfs.set_patterns_length_values()
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt_stops     bigint;
  cnt_patterns  bigint;
BEGIN
  RAISE NOTICE 'Calculating nw_vs_shape_coeff in stage_gtfs.pattern_stops ...';

  WITH
    updated_stops AS (
      UPDATE stage_gtfs.pattern_stops AS upd
      SET nw_vs_shape_coeff = CASE
                                WHEN shape_geom IS NULL THEN NULL
                                WHEN ST_Length(shape_geom) = 0.0 THEN 0.0
                                ELSE ST_Length(vpsg.geom) / ST_Length(shape_geom)
                              END
      FROM (
        SELECT ptid, stop_seq, geom
        FROM stage_gtfs.view_pattern_stops_geom
      ) AS vpsg
      WHERE upd.ptid = vpsg.ptid AND upd.stop_seq = vpsg.stop_seq
      RETURNING *
    )
  SELECT INTO cnt_stops count(*) FROM updated_stops;

  RAISE NOTICE 'Calculating shape and nw total lengths in stage_gtfs.patterns ...';

  WITH
    updated_patterns AS (
      UPDATE stage_gtfs.patterns AS upd
      SET
        shape_len_total   = vpsg.shape_len_total,
        nw_len_total      = vpsg.nw_len_total,
        nw_vs_shape_coeff = CASE
                              WHEN vpsg.shape_len_total IS NULL THEN NULL
                              WHEN vpsg.shape_len_total = 0.0 THEN 0.0
                              ELSE vpsg.nw_len_total / vpsg.shape_len_total
                            END
      FROM (
        SELECT
          ptid,
          sum( ST_Length(shape_geom) )  AS shape_len_total,
          sum( ST_Length(geom) )        AS nw_len_total
        FROM stage_gtfs.view_pattern_stops_geom
        GROUP BY ptid
      ) AS vpsg
      WHERE upd.ptid = vpsg.ptid
      RETURNING *
    )
  SELECT INTO cnt_patterns count(*) FROM updated_patterns;

  RETURN QUERY
  SELECT 'stage_gtfs.pattern_stops' AS table_name, cnt_stops AS rows_affected
  UNION
  SELECT 'stage_gtfs.patterns' AS table_name, cnt_patterns AS rows_affected;
END;
$$;
