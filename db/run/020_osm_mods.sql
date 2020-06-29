/*
 * Combine bus and tram OSM ways,
 * and fix some common errors in the data.
 * After this you have stage_osm.combined_lines
 * where you can check for and fix possible errors.
 */

\set ON_ERROR_STOP on

BEGIN;

SELECT stage_osm.populate_combined_lines();
SELECT stage_osm.split_ring_geoms_combined_lines();
SELECT stage_osm.fix_unconnected_combined_lines();

COMMIT;
