/*
 * Preparing stop times for interpolation along the network.
 * We have to keep meaningful dep and arr times to use them as
 * reference points for the interpolation, while getting rid of
 * repetitive ones that would give us zero driving times between stops.
 * In other words, we look for meaningful "partition boundaries"
 * within stop sequences.
 *
 * Some prerequisites:
 * - timepoint is true -> these must always be partition boundaries
 * - first or last stop of a template -> always boundaries as well
 *
 * As a basic rule, if a stop time is different from the previous one,
 * then it should start a new partition. But this is not as simple as that
 * since we have both arrival and departure times to deal with,
 * and reflecting backwards to the previous stop OR forwards to the next one
 * is not enough.
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
    WHERE ttid = '1069_2_1'
  ),

  nextprev_marked AS (
    SELECT
      *,
      lag(dep) OVER (PARTITION BY ttid ORDER BY stop_seq)   AS prev_dep,
      lead(arr) OVER (PARTITION BY ttid ORDER BY stop_seq)  AS next_arr
    FROM tt_arr_unnested
  ),

  times_to_fix_marked AS (
    SELECT
      ttid,
      stop_seq,
      timepoint,
      nullif(arr, dep) AS arr,
      dep
    FROM nextprev_marked
  )--,
  select * from times_to_fix_marked;
/*
  times_fixed_arr AS (
    SELECT
      ttid,
      array_agg(stop_seq ORDER BY stop_seq)   AS stop_sequences,
      array_agg(
        CASE
          WHEN fix_arr > 0 THEN arr + (dep - arr) / 2
          ELSE arr
        END
      ORDER BY stop_seq)                      AS arr_time_diffs,
      array_agg(dep ORDER BY stop_seq)        AS dep_time_diffs,
      sum(fix_arr)                            AS n_fixed_times
    FROM times_to_fix_marked
    GROUP BY ttid
  )

SELECT ttid, n_fixed_times FROM times_fixed_arr WHERE n_fixed_times > 0 ORDER BY n_fixed_times DESC, ttid;
*/
