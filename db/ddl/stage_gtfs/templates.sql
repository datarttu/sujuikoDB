/*
 * TODO: Add / redesign the sched.templates and sched.segment_times staging stuff,
 *       finally delete trip_template_arrays.
 */

CREATE TABLE stage_gtfs.trip_template_arrays (
  /*
   * This will be just a surrogate pkey with a running number.
   */
  ttid            text          PRIMARY KEY,
  /*
   * These define a unique record:
   */
  route_id        text,
  direction_id    smallint,
  shape_id        text,
  stop_ids        integer[],
  stop_sequences  smallint[],
  rel_distances   double precision[],
  arr_time_diffs  interval[],
  dep_time_diffs  interval[],
  timepoints      boolean[],
  /*
   * These describe to which individual trips the above attributes apply:
   */
  trip_ids        text[],
  service_ids     text[],
  start_times     interval[],
  dates           date[],
  route_found     boolean,
  UNIQUE (route_id, direction_id, shape_id, stop_ids, stop_sequences,
          rel_distances, arr_time_diffs, dep_time_diffs, timepoints)
);
COMMENT ON TABLE stage_gtfs.trip_template_arrays IS
'"Compressed" trip templates from GTFS trips and stop times.
GTFS trips that share identical
- route and direction ids,
- trip shape geometry,
- stop ids and their order,
- relative trip distances at stops,
- stop times calculated as differences from the first stop, and
- timepoint flags
are grouped into one record.
Stop time attributes, as well as the dates and
initial departure times to which the trip template applies,
are stored as arrays that can be later decomposed into rows.
From this table, the records that are successfully routable
on the network can be transferred to the production schedule tables.
Note that this table should already use the 1-2 direction id system
instead of GTFS 0-1 standard.
route_found is populated in a later stage, indicating whether the trip template
has a complete route on the network and can be transferred to sched schema.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_trip_template_arrays()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       bigint;
BEGIN
  DELETE FROM stage_gtfs.trip_template_arrays;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.trip_template_arrays', cnt;

  WITH
    stoptime_arrays AS (
      SELECT
        trip_id,
        array_agg(stop_id ORDER BY stop_sequence)           AS stop_ids,
        array_agg(stop_sequence ORDER BY stop_sequence)     AS stop_sequences,
        array_agg(rel_dist_traveled ORDER BY stop_sequence) AS rel_distances,
        array_agg(arr_time_diff ORDER BY stop_sequence)     AS arr_time_diffs,
        array_agg(dep_time_diff ORDER BY stop_sequence)     AS dep_time_diffs,
        array_agg(timepoint ORDER BY stop_sequence)         AS timepoints
      FROM stage_gtfs.normalized_stop_times
      GROUP BY trip_id
    ),
    compressed_arrays AS (
      SELECT
        twd.route_id,
        /*
         * NOTE: We do the conversion of direction id
         *       from GTFS 0-1 to HFP 1-2 system here.
         */
        twd.direction_id + 1                                AS dir,
        twd.shape_id,
        sa.stop_ids,
        sa.stop_sequences,
        sa.rel_distances,
        sa.arr_time_diffs,
        sa.dep_time_diffs,
        sa.timepoints,
        array_agg(twd.trip_id ORDER BY twd.trip_id)         AS trip_ids,
        array_agg(twd.service_id ORDER BY twd.trip_id)      AS service_ids,
        array_agg(twd.trip_start_hms ORDER BY twd.trip_id)  AS start_times
      FROM stoptime_arrays                    AS sa
      INNER JOIN stage_gtfs.trips_with_dates  AS twd
      ON sa.trip_id = twd.trip_id
      GROUP BY
        twd.route_id,
        dir,
        twd.shape_id,
        sa.stop_ids,
        sa.stop_sequences,
        sa.rel_distances,
        sa.arr_time_diffs,
        sa.dep_time_diffs,
        sa.timepoints
    )
  INSERT INTO stage_gtfs.trip_template_arrays (
    ttid, route_id, direction_id, shape_id, stop_ids, stop_sequences,
    rel_distances, arr_time_diffs, dep_time_diffs, timepoints,
    trip_ids, service_ids, start_times
  )
  SELECT
    concat_ws(
      '_',
      route_id,
      dir,
      row_number() OVER (PARTITION BY route_id, dir)
    ) AS ttid,
    *
  FROM compressed_arrays;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into stage_gtfs.trip_template_arrays', cnt;

  /*
   * The start_times arrays created above still contain duplicated and
   * unsorted values, fix it here.
   */
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

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'Start time arrays with unique values updated for % rows in stage_gtfs.trip_template_arrays', cnt;

  WITH
    ttids_tripids AS (
      SELECT
        ttid,
        unnest(trip_ids) AS trip_id
      FROM stage_gtfs.trip_template_arrays
    ),
    ttid_all_dates AS (
      SELECT
        tt.ttid,
        unnest(twd.dates) AS valid_date
      FROM ttids_tripids                      AS tt
      INNER JOIN stage_gtfs.trips_with_dates  AS twd
      ON tt.trip_id = twd.trip_id
    ),
    ttid_uniq_dates AS (
      SELECT DISTINCT ttid, valid_date
      FROM ttid_all_dates
      ORDER BY ttid, valid_date
    )
  UPDATE stage_gtfs.trip_template_arrays AS tta
  SET dates = da.dates
  FROM (
    SELECT
      ttid,
      array_agg(valid_date) AS dates
    FROM ttid_uniq_dates
    GROUP BY ttid
  ) AS da
  WHERE tta.ttid = da.ttid;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows updated with dates array', cnt;

  RETURN 'OK';
END;
$$;
