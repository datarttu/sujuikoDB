CREATE TABLE stage_nw.trip_template_routes (
  ttid            text          NOT NULL,
  stop_seq        smallint      NOT NULL,
  path_seq        integer       NOT NULL,
  edge            integer,
  inode           integer,
  jnode           integer,
  PRIMARY KEY (ttid, stop_seq, path_seq)
);
COMMENT ON TABLE stage_nw.trip_template_routes IS
'Trip templates, as in stage_gtfs.trip_template_arrays,
routed on the network of nw.links & nw.nodes, meaning that the stop id sequences
are decomposed into nodes and thereby edges between the stops.
Missing edges of failed routes are NOT included here.
This table is then used together with stage_gtfs.trip_template_arrays
to populate sched.trip_templates and sched.segments with trip templates
that have complete routes on the network.';

CREATE OR REPLACE FUNCTION stage_nw.populate_trip_template_routes()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       bigint;
BEGIN
  DELETE FROM stage_nw.trip_template_routes;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_nw.trip_template_routes', cnt;

  WITH
    tta_unnested AS (
      SELECT
        ttid,
        unnest(stop_sequences)  AS stop_seq,
        unnest(stop_ids)        AS stop_id
      FROM stage_gtfs.trip_template_arrays
    ),
    tt_with_nodes AS (
      SELECT
        tt.ttid       AS ttid,
        tt.stop_seq   AS stop_seq,
        s.nodeid      AS nodeid
      FROM tta_unnested     AS tt
      INNER JOIN nw.stops   AS s
      ON tt.stop_id = s.stopid
    ),
    tt_nodepairs AS (
      SELECT
        ttid,
        stop_seq,
        nodeid                                                  AS start_node,
        lead(nodeid) OVER (PARTITION BY ttid ORDER BY stop_seq) AS end_node
      FROM tt_with_nodes
    ),
    /*
     * Add both enter and exit node to each edge so we know to which
     * direction the edge is traversed.
     */
    jnoded_node_pair_routes AS (
      SELECT
        start_node,
        end_node,
        path_seq,
        edge,
        node  AS inode,
        lead(node) OVER (
          PARTITION BY start_node, end_node ORDER BY path_seq
        )     AS jnode
      FROM stage_nw.node_pair_routes
    ),
    /*
     * The last stops of each trip template do not occur in the above set
     * since there are no edges after them.
     * However, we want to add them "artificially" to the set.
     * This way they are directly included in later joins,
     * and NULL values in those joins can be used to detect route parts
     * that are actually missing from between trip template stops.
     */
    tt_nodepairs_with_laststops AS (
      SELECT
        ttnp.ttid,
        ttnp.stop_seq,
        npr.path_seq,
        npr.edge,
        npr.inode,
        npr.jnode
      FROM tt_nodepairs                    AS ttnp
      LEFT JOIN jnoded_node_pair_routes    AS npr
      ON ttnp.start_node = npr.start_node
        AND ttnp.end_node = npr.end_node
      WHERE npr.edge <> -1
      UNION
      SELECT
        ttid,
        max(stop_seq)   AS stop_seq,
        0::integer      AS path_seq,
        -1::integer     AS edge,
        -1::integer     AS inode,
        -1::integer     AS jnode
      FROM tt_nodepairs
      GROUP BY ttid
     )
  INSERT INTO stage_nw.trip_template_routes (
   ttid, stop_seq, path_seq, edge, inode, jnode
  )
  SELECT * FROM tt_nodepairs_with_laststops
  ORDER BY ttid, stop_seq, path_seq;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into stage_nw.trip_template_routes', cnt;

  /*
   * To ensure consistency between the routes found above
   * and the source array table, we update the route_found field
   * of the array table right away.
   */

  UPDATE stage_gtfs.trip_template_arrays
  SET route_found = false;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'route_found set to false for all % records in stage_gtfs.trip_template_arrays', cnt;

  WITH
    tta AS (
      SELECT
        ttid,
        unnest(stop_sequences) AS stop_seq
      FROM stage_gtfs.trip_template_arrays
    ),
    ttids_with_missing_paths AS (
      SELECT
        tta.ttid,
        bool_and(ttr.edge IS NOT NULL) AS path_complete
      FROM tta
      LEFT JOIN stage_nw.trip_template_routes AS ttr
        ON tta.ttid = ttr.ttid
        AND tta.stop_seq = ttr.stop_seq
      GROUP BY tta.ttid
    )
  UPDATE stage_gtfs.trip_template_arrays  AS upd
  SET route_found = true
  FROM ttids_with_missing_paths           AS twmp
  WHERE upd.ttid = twmp.ttid
    AND twmp.path_complete IS true;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'Complete route found for % trip templates', cnt;
  RAISE NOTICE '(route_found set to true for them in stage_gtfs.trip_template_arrays)';

  RETURN 'OK';
END;
$$;
