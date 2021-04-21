/*
 * Build the "contracted" network for nw staging,
 * i.e. a network where redundant links on continuous sections
 * between intersections are merged together.
 *
 * NOTE:  If you have to fix something in the original network,
 *        modify stage_osm.combined_lines
 *        and then run this and the next scripts again.
 */

\set ON_ERROR_STOP on

BEGIN;

SELECT * FROM stage_nw.populate_raw_nw();
SELECT stage_nw.analyze_inout_edges();
SELECT stage_nw.build_contracted_network();

COMMIT;
