/*
 * Are there trip templates where the last stop time in minutes occurs
 * already before the last stop?
 */
WITH
  tt_arr_unnested AS (
    SELECT
     ttid,
     unnest(stop_sequences)  AS stop_seq,
     unnest(timepoints)      AS timepoint,
     unnest(arr_time_diffs)  AS arr,
     unnest(dep_time_diffs)  AS dep
    FROM stage_gtfs.trip_template_arrays
  ),
  tt_lastones_marked AS (
    SELECT
      *,
      max(stop_seq) OVER (PARTITION BY ttid) AS last_seq,
      max(arr) OVER (PARTITION BY ttid) AS max_arr
    FROM tt_arr_unnested
  ),
  tt_same_with_lastone_marked AS (
    SELECT
      *,
      (dep = max_arr AND stop_seq <> last_seq)::int AS same_with_last
    FROM tt_lastones_marked
  ),
  tt_with_duplicate_lasttimes AS (
    SELECT
      *,
      sum(same_with_last) OVER (PARTITION BY ttid) AS n_duplicates
    FROM tt_same_with_lastone_marked
  )
SELECT * FROM tt_with_duplicate_lasttimes
WHERE n_duplicates > 1
ORDER BY ttid, stop_seq
LIMIT 70;

/*
 * 6.5.2020
 * Max number of "last duplicates" seems to be 3, including the last stop.
 * This occurs at least with route 1093K.
 */
