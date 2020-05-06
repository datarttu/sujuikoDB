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

BEGIN;

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

  tt_routes AS (
    SELECT
      s.ttid,
      s.stop_seq,
      r.path_seq,
      s.arr,
      s.dep,
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
      ttid,
      stop_seq,
      path_seq,
      /*
       * We remove stop times from anywhere but timepoint stop rows.
       * Also, we remove arr time from the first stop
       * and dep time from the last stop,
       * so at least the intermediate result is a bit more clear for debugging.
       */
      CASE
        WHEN i_strict AND stop_seq > 1 THEN arr
        ELSE NULL
      END                                               AS arr,
      CASE
        WHEN i_strict
          AND stop_seq < (max(stop_seq) OVER (PARTITION BY ttid))
          THEN dep
        ELSE NULL END                                   AS dep,
      linkid,
      inode,
      jnode,
      seg_len,
      sum(seg_len) OVER (
        PARTITION BY ttid ORDER BY stop_seq, path_seq)  AS cumul_len,
      i_stop,
      lead(i_stop) OVER (
        PARTITION BY ttid ORDER BY stop_seq, path_seq)  AS j_stop,
      i_strict,
      lead(i_strict) OVER (
        PARTITION BY ttid ORDER BY stop_seq, path_seq)  AS j_strict,
      sum(i_strict::int) OVER (
        PARTITION BY ttid ORDER BY stop_seq, path_seq)  AS part_num
    FROM tt_routes
  ),

  prepare_partition_aggregates AS (
    SELECT
      ttid,
      part_num,
      /*
       * Using min / max here should not make any difference,
       * we just need the single values from the partition start.
       */
      min(arr)      AS part_arr,
      min(dep)      AS part_dep,
      sum(seg_len)  AS part_len
    FROM tt_route_partitions
    GROUP BY ttid, part_num
  ),

  partition_aggregates AS (
    SELECT
      ttid,
      part_num,
      part_len,
      part_dep      AS part_i_time,
      lead(part_arr) OVER (
        PARTITION BY ttid ORDER BY part_num
      )             AS part_j_time,
      /*
       * Note that possible waiting times at stops, i.e. dep - arr,
       * are NOT included in the partition total driving times.
       * They are eventually taken into account at segment level.
       */
      lead(part_arr) OVER (
        PARTITION BY ttid ORDER BY part_num
      ) - part_dep  AS part_total_time
    FROM prepare_partition_aggregates
  ),

  prepare_partition_segments AS (
    SELECT
      rp.ttid,
      rp.stop_seq,
      rp.path_seq,
      rp.linkid,
      rp.inode,
      rp.jnode,
      rp.seg_len,
      rp.cumul_len,
      rp.i_stop,
      rp.j_stop,
      rp.i_strict,
      rp.j_strict,
      pa.part_num,
      pa.part_len,
      pa.part_i_time,
      pa.part_j_time,
      pa.part_total_time,
      pa.part_i_time + (sum(rp.seg_len) OVER (
        PARTITION BY rp.ttid, pa.part_num ORDER BY rp.stop_seq, rp.path_seq
      ) / pa.part_len) * pa.part_total_time   AS j_time,
      rp.cumul_len / (sum(rp.seg_len) OVER (
        PARTITION BY rp.ttid
      ))                                      AS j_rel_dist
    FROM tt_route_partitions        AS rp
    INNER JOIN partition_aggregates AS pa
      ON rp.ttid = pa.ttid AND rp.part_num = pa.part_num
  ),

  partition_segments AS (
    SELECT
      *,
      coalesce(
        lag(j_time) OVER (
          PARTITION BY ttid, part_num ORDER BY stop_seq, path_seq),
        part_i_time
      ) AS i_time,
      coalesce(
        lag(j_rel_dist) OVER (
          PARTITION BY ttid ORDER BY stop_seq, path_seq),
        0
      ) AS i_rel_dist
    FROM prepare_partition_segments
  )

INSERT INTO sched.segments (
  ttid, linkid, i_node, j_node, i_time, j_time, i_stop, j_stop,
  i_strict, j_strict, i_rel_dist, j_rel_dist
)
SELECT
  ttid, linkid, inode, jnode, i_time, j_time, i_stop, j_stop,
  i_strict, j_strict, i_rel_dist, j_rel_dist
FROM partition_segments
WHERE path_seq > 0
ORDER BY ttid, i_time, linkid;
