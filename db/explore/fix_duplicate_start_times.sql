/*
 * start_times arrays in stage_gtfs.trip_template_arrays contain
 * duplicate values, and the values are not ordered.
 */

BEGIN;
WITH
  unnested_times AS (
    SELECT
      ttid,
      unnest(start_times) AS start_time
    FROM stage_gtfs.trip_template_arrays
  ),
  unique_times AS (
    SELECT DISTINCT ttid, start_time
    FROM unnested_times
  ),
  new_time_arrays AS (
    SELECT
      ttid,
      array_agg(start_time ORDER BY start_time) AS start_times
    FROM unique_times
    GROUP BY ttid
  )
UPDATE stage_gtfs.trip_template_arrays  AS tta
SET start_times = nta.start_times
FROM new_time_arrays                    AS nta
WHERE tta.ttid = nta.ttid;

ROLLBACK;
