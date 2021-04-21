/*
 * Fix some common errors in stage_osm.combined_lines.
 */

\set ON_ERROR_STOP on

BEGIN;

SELECT stage_osm.split_ring_geoms_combined_lines();
SELECT stage_osm.fix_unconnected_combined_lines();

COMMIT;
