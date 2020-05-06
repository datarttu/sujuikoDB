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
      AND route_id LIKE '1077%' -- REMOVE THIS AFTER EXPERIMENTING
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
  ),

  tt_routes AS (
    SELECT
      s.ttid,
      s.stop_seq,
      r.path_seq,
      l.linkid,
      r.inode,
      r.jnode,
      ST_Length(l.geom)                       AS seg_len,
      r.path_seq IN (0, 1)                    AS i_stop,
      s.timepoint AND (r.path_seq IN (0, 1))  AS i_strict
    FROM tt_set_firstlast_timepoints          AS s
    INNER JOIN stage_nw.trip_template_routes  AS r
      ON s.ttid = r.ttid AND s.stop_seq = r.stop_seq
    /*
     * Left join, because we want to keep the last stops of each template:
     * they do not start a segment so they get no linkid.
     */
    LEFT JOIN nw.links                        AS l
      ON r.edge = l.linkid
  ),

  tt_route_partitions AS (
    SELECT
      *,
      sum(i_strict::int) OVER (
        PARTITION BY ttid ORDER BY stop_seq, path_seq
      ) AS part_num
    FROM tt_routes
  )

SELECT *
FROM tt_route_partitions
ORDER BY ttid, stop_seq, path_seq
LIMIT 60;
