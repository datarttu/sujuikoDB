/*
 * Combine bus and tram OSM ways.
 * After this you have stage_osm.combined_lines
 * where you can check for and fix possible errors.
 */

\set ON_ERROR_STOP on

BEGIN;

SELECT stage_osm.populate_combined_lines();

COMMIT;
