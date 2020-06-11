DROP TABLE IF EXISTS stage_gtfs.templates CASCADE;
CREATE TABLE stage_gtfs.templates (
  ttid              text          PRIMARY KEY,
  ptid              text          NOT NULL REFERENCES stage_gtfs.patterns(ptid),
  arr_times         interval[],
  dep_times         interval[],
  stop_seqs         smallint[],
  trip_ids          text[]
);
COMMENT ON TABLE stage_gtfs.templates IS
'Staging table for `sched.templates`.
Each template `ttid` represents a variant of pattern `ptid`: templates of the same `ptid`
share same stops and network paths but differ in stop times relative to trip start time.
E.g., a `ptid` may have different templates during morning peak and off-peak times.
- `arr_times`, `dep_times`, `stop_seqs`: These are filled when the table is populated for the first time,
  and they should be of same size. `.template_stops` is then populated using these values.
- `trip_ids`: original GTFS `trip_id` records the template was built from';

DROP TABLE IF EXISTS stage_gtfs.template_timestamps CASCADE;
CREATE TABLE stage_gtfs.template_timestamps (
  ttid              text          NOT NULL,
  start_timestamp   timestamptz   NOT NULL,
  PRIMARY KEY (ttid, start_timestamp)
);
COMMENT ON TABLE stage_gtfs.template_timestamps IS
'Absolute timestamps of scheduled departure times of trips that belong to
the template `ttid`. The timestamps could alternatively be stored in an array field
per `ttid` in `.templates`, but this could result in very large arrays.';

DROP TABLE IF EXISTS stage_gtfs.template_stops CASCADE;
CREATE TABLE stage_gtfs.template_stops (
  ttid            text              NOT NULL REFERENCES stage_gtfs.templates(ttid),
  ij_stop_seqs    smallint[]        NOT NULL CHECK (cardinality(ij_stop_seqs) = 2),
  ij_times        interval[]        NOT NULL CHECK (cardinality(ij_times) = 2),
  ij_seconds      double precision  NOT NULL CHECK (ij_seconds > 0.0),

  PRIMARY KEY (ttid, ij_stop_seqs)
);
COMMENT ON TABLE stage_gtfs.template_stops IS
'Stop times (relative to trip start time) of templates `ttid` as [from_stop-to_stop] arrays,
where both stops are time equalization stops, i.e., `timepoint IS true` for them.
The terminus stop of a trip is always treated as timepoint stop.
This means that not even nearly every of a pattern `ptid` is included here.
Non-timepoint stops, as well as between-stop segments, get their time values
as interpolated along the network between the timepoint stops.
- `ij_stop_seqs`: `stop_seq` values of start and end timepoint stops of the pattern part,
  referring to `.pattern_stops (stop_seq)`, except that `.pattern_stops` does not include
  the last `stop_seq` of the pattern directly but it has to be generated.
- `ij_times`: timepoint stop times';

DROP FUNCTION IF EXISTS stage_gtfs.extract_trip_templates(text);
CREATE OR REPLACE FUNCTION stage_gtfs.extract_trip_templates(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt_templates   bigint;
  cnt_timestamps  bigint;
  cnt_stops       bigint;
BEGIN
  RAISE NOTICE 'Extracting trip template patterns to stage_gtfs.templates ...';

  WITH
    trips_with_ptid AS (
      SELECT
        ptid,
        unnest(trip_ids)  AS trip_id
      FROM stage_gtfs.patterns
    ),
    all_stop_times AS (
      SELECT
        trip_id,
        arr_time_diff AS arr,
        dep_time_diff AS dep,
        stop_sequence AS stop_seq,
        -- Catch also terminus stops as timepoints:
        (timepoint IS true
          OR stop_sequence = max(stop_sequence) OVER (PARTITION BY trip_id)
        )                 AS timepoint
      FROM stage_gtfs.normalized_stop_times
    ),
    timepoint_arrays AS (
      SELECT
        twp.ptid,
        ast.trip_id,
        array_agg(ast.arr ORDER BY ast.stop_seq)      AS arr_times,
        array_agg(ast.dep ORDER BY ast.stop_seq)      AS dep_times,
        array_agg(ast.stop_seq ORDER BY ast.stop_seq) AS stop_seqs
      FROM all_stop_times         AS ast
      INNER JOIN trips_with_ptid  AS twp
        ON ast.trip_id = twp.trip_id
      WHERE ast.timepoint IS true
      GROUP BY twp.ptid, ast.trip_id
    )
  INSERT INTO stage_gtfs.templates (
    ttid, ptid, arr_times, dep_times, stop_seqs, trip_ids
  )
  SELECT
    ptid || '_' || row_number() OVER (
      PARTITION BY ptid ORDER BY ptid)  AS ttid,
    ptid,
    arr_times,
    dep_times,
    stop_seqs,
    array_agg(trip_id)  AS trip_ids
  FROM timepoint_arrays
  GROUP BY ptid, arr_times, dep_times, stop_seqs;

  GET DIAGNOSTICS cnt_templates = ROW_COUNT;

  RAISE NOTICE 'Extracting start timestamps to stage_gtfs.template_timestamps ...';

  WITH
    unnest_tripids AS (
      SELECT
        ttid,
        unnest(trip_ids)  AS trip_id
      FROM stage_gtfs.templates
    ),
    unnest_dates AS (
      SELECT
        ut.ttid,
        ut.trip_id,
        (unnest(twd.dates) || ' Europe/Helsinki')::timestamptz + twd.trip_start_hms AS start_timestamp
      FROM unnest_tripids                     AS ut
      INNER JOIN stage_gtfs.trips_with_dates  AS twd
        ON ut.trip_id = twd.trip_id
    )
  INSERT INTO stage_gtfs.template_timestamps (ttid, start_timestamp)
  SELECT DISTINCT ttid, start_timestamp
  FROM unnest_dates
  ORDER BY ttid, start_timestamp;

  GET DIAGNOSTICS cnt_timestamps = ROW_COUNT;

  RAISE NOTICE 'Populating stage_gtfs.template_stops from .templates arrays ...';

  WITH
    unnest_arrays AS (
      SELECT
        ttid,
        unnest(arr_times) AS arr,
        unnest(dep_times) AS dep,
        unnest(stop_seqs) AS stop_seq
      FROM stage_gtfs.templates
    ),
    all_pairs AS (
      SELECT
        ttid,
        ARRAY[stop_seq, lead(stop_seq) OVER w_ttid]::smallint[] AS ij_stop_seqs,
        ARRAY[dep, lead(arr) OVER w_ttid]::interval[]           AS ij_times
      FROM unnest_arrays
      WINDOW w_ttid AS (PARTITION BY ttid ORDER BY stop_seq)
    )
  INSERT INTO stage_gtfs.template_stops (
    ttid, ij_stop_seqs, ij_times, ij_seconds
  )
  SELECT
    ttid,
    ij_stop_seqs,
    ij_times,
    extract(epoch FROM ij_times[2] - ij_times[1])::double precision AS ij_seconds
  FROM all_pairs
  WHERE ij_stop_seqs[2] IS NOT NULL
  ORDER BY ttid, ij_stop_seqs;

  GET DIAGNOSTICS cnt_stops = ROW_COUNT;

  RETURN QUERY
  SELECT 'stage_gtfs.templates' AS table_name, cnt_templates AS rows_affected
  UNION
  SELECT 'stage_gtfs.template_timestamps' AS table_name, cnt_timestamps AS rows_affected
  UNION
  SELECT 'stage_gtfs.template_stops' AS table_name, cnt_stops AS rows_affected;
END;
$$;

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
