/*
 * Corrections on raw OSM data after importing.
 */
\set ON_ERROR_STOP on

BEGIN;

/*
 * Fix roundabouts that have been modeled as two-way.
 */
UPDATE stage_osm.raw_bus_lines
SET oneway = 'yes'
WHERE junction LIKE 'roundabout';

COMMIT;
