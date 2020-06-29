/*
 * Snap GTFS stops to the contracted network,
 * group stops close to each other
 * and snap stops near link ends to the end nodes.
 * Then populate the nw schema.
 *
 * NOTE:  If there are errors in stop locations,
 *        modify stage_gtfs.stops_with_mode
 *        and run this and the next scripts again.
 */
\set ON_ERROR_STOP on

BEGIN;

SELECT stage_nw.snap_stops_to_network();
SELECT stage_nw.delete_outlier_stops();
SELECT stage_nw.snap_stops_near_nodes();
SELECT stage_nw.cluster_stops_on_edges();
SELECT stage_nw.populate_nw_links();
SELECT nw.create_node_table();
SELECT stage_nw.populate_nw_stops();

COMMIT;
