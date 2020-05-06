/*
 * We make a significant set of modifications to the trip templates
 * when transferring them to `sched` schema:
 * -  Only valid and meaningful templates are transferred:
 *    see the WHERE clause in the first part of the CTE.
 * -  Stop times are only used if the stop is defined as timepoint.
 *    Moreover, the first and last stop of the template is always set as timepoint.
 *    Segment times, including non-timepoint stop times, will be interpolated
 *    between these timepoint stops.
 * -  Stop sequences are decomposed into route segments,
 *    and segment values are calculated using "partitions".
 *    A partition = route part between timepoint stops.
 */

WITH

  tt_arr_unnested AS (
    SELECT
      ttid,
      unnest(stop_sequences)  AS stop_seq,
      unnest(arr_time_diffs)  AS arr,
      unnest(dep_time_diffs)  AS dep,
      unnest(timepoints)      AS timepoint
    FROM stage_gtfs.trip_template_arrays
    WHERE route_found IS true
      AND start_times IS NOT NULL
      AND dates IS NOT NULL
      AND route_id = '1001' -- REMOVE THIS FILTER FOR PRODUCTION!!
  ),

  tt_set_firstlast_timepoints AS (
    SELECT ttid, stop_seq, arr, dep,
      CASE
        WHEN stop_seq = 1 THEN true
        WHEN stop_seq = max(stop_seq) OVER (PARTITION BY ttid) THEN true
        ELSE timepoint
      END AS timepoint
    FROM tt_arr_unnested
  ),

  tt_discard_nontimepoint_times AS (
    SELECT ttid, stop_seq,
      CASE WHEN timepoint THEN arr ELSE NULL END AS arr,
      CASE WHEN timepoint THEN dep ELSE NULL END AS dep,
      timepoint
    FROM tt_set_firstlast_timepoints
  )

SELECT *
FROM tt_discard_nontimepoint_times
ORDER BY ttid, stop_seq
LIMIT 60;
