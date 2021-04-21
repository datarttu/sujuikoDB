DROP TABLE IF EXISTS stage_gtfs.templates CASCADE;
CREATE TABLE stage_gtfs.templates (
  ttid              text          PRIMARY KEY,
  ptid              text          NOT NULL REFERENCES stage_gtfs.patterns(ptid),
  arr_times         interval[],
  dep_times         interval[],
  stop_seqs         smallint[],
  trip_ids          text[],
  invalid_reasons   text[]        NOT NULL DEFAULT '{}'
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
  ij_seconds      double precision  NOT NULL,
  invalid_reasons text[]            NOT NULL DEFAULT '{}',

  PRIMARY KEY (ttid, ij_stop_seqs)
);
COMMENT ON TABLE stage_gtfs.template_stops IS
'Stop times (relative to trip start time) of templates `ttid` as [from_stop-to_stop] arrays,
where both stops are time equalization stops, i.e., `timepoint IS true` for them.
The terminus stop of a trip is always treated as timepoint stop.
This means that not even nearly every stop of a pattern `ptid` is included here.
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
  GROUP BY ptid, arr_times, dep_times, stop_seqs
  ON CONFLICT DO NOTHING;

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
  ORDER BY ttid, start_timestamp
  ON CONFLICT DO NOTHING;

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
  ORDER BY ttid, ij_stop_seqs
  ON CONFLICT DO NOTHING;

  GET DIAGNOSTICS cnt_stops = ROW_COUNT;

  RETURN QUERY
  SELECT 'stage_gtfs.templates' AS table_name, cnt_templates AS rows_affected
  UNION
  SELECT 'stage_gtfs.template_timestamps' AS table_name, cnt_timestamps AS rows_affected
  UNION
  SELECT 'stage_gtfs.template_stops' AS table_name, cnt_stops AS rows_affected;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.extract_trip_templates(text) IS
'Populates `stage_gtfs.template_*` tables.
- Source tables: `stage_gtfs.patterns`,
                 `stage_gtfs.normalized_stop_times`,
                 `stage_gtfs.trips_with_dates`
- Target tables: `stage_gtfs.templates`,
                 `stage_gtfs.template_timestamps`,
                 `stage_gtfs.template_stops`';

DROP FUNCTION IF EXISTS stage_gtfs.transfer_templates(text);
CREATE OR REPLACE FUNCTION stage_gtfs.transfer_templates(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RAISE NOTICE 'Transferring stage_gtfs.templates to sched.templates ...';
  IF NOT EXISTS (SELECT * FROM sched.patterns LIMIT 1) THEN
    RAISE WARNING 'sched.patterns is empty, populate it first!';
  END IF;
  RETURN QUERY
  WITH
    inserted AS (
      INSERT INTO sched.templates (
        ttid, ptid, gtfs_trip_ids
      )
      SELECT
        t.ttid, t.ptid, t.trip_ids AS gtfs_trip_ids
      FROM stage_gtfs.templates AS t
      INNER JOIN sched.patterns AS p
        ON t.ptid = p.ptid
      WHERE cardinality(t.invalid_reasons) = 0
      RETURNING *
    )
  SELECT 'sched.templates' AS table_name, count(*) AS rows_affected
  FROM inserted;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.transfer_templates(text) IS
'Populate `sched.templates` with valid records from `stage_gtfs.templates`.
- `where_sql`: NOT IMPLEMENTED YET
- Source tables:  `stage_gtfs.templates`,
                  `sched.patterns`
- Target tables:  `sched.templates`';

DROP FUNCTION IF EXISTS stage_gtfs.transfer_template_timestamps(text);
CREATE OR REPLACE FUNCTION stage_gtfs.transfer_template_timestamps(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RAISE NOTICE 'Transferring stage_gtfs.template_timestamps to sched.template_timestamps ...';
  IF NOT EXISTS (SELECT * FROM sched.templates LIMIT 1) THEN
    RAISE WARNING 'sched.templates is empty, populate it first!';
  END IF;
  RETURN QUERY
  WITH
    inserted AS (
      INSERT INTO sched.template_timestamps (
        ttid, start_ts
      )
      SELECT
        tt.ttid, tt.start_timestamp AS start_ts
      FROM stage_gtfs.template_timestamps AS tt
      INNER JOIN sched.templates          AS t
        ON tt.ttid = t.ttid
      RETURNING *
    )
  SELECT 'sched.template_timestamps' AS table_name, count(*) AS rows_affected
  FROM inserted;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.transfer_templates(text) IS
'Populate `sched.template_timestamps` with valid records from stage_gtfs.template_timestamps`
that have a corresponding `ttid` record in `sched.templates`.
- `where_sql`: NOT IMPLEMENTED YET
- Source tables:  `stage_gtfs.template_timestamps`,
                  `sched.templates`
- Target tables:  `sched.template_timestamps`';

DROP FUNCTION IF EXISTS stage_gtfs.transfer_segment_times(text);
CREATE OR REPLACE FUNCTION stage_gtfs.transfer_segment_times(where_sql text DEFAULT NULL)
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RAISE NOTICE 'Populating sched.segment_times ...';
  IF NOT EXISTS (SELECT * FROM sched.templates LIMIT 1) THEN
    RAISE WARNING 'sched.templates is empty, populate it first!';
  END IF;
  RETURN QUERY
  WITH
    timepoint_spans_to_segs AS (
      SELECT
        ts.ttid,
        sg.segno,
        ts.ij_seconds,
        ts.ij_times,
        ts.ij_stop_seqs,
        rn_length(sg.ij_dist_span)    AS seg_dist,
        max(upper(sg.ij_dist_span)) OVER (
          PARTITION BY ts.ttid, ts.ij_stop_seqs
          )                           AS section_dist,
        ARRAY[
          sg.stop_seq = ts.ij_stop_seqs[1]
          AND sg.ij_stops[1] IS NOT NULL,
          sg.stop_seq = ts.ij_stop_seqs[2] - 1
          AND sg.ij_stops[2] IS NOT NULL
        ]                             AS ij_timepoints
      FROM stage_gtfs.template_stops  AS ts
      INNER JOIN sched.templates      AS tp
        ON  ts.ttid = tp.ttid
      INNER JOIN sched.segments       AS sg
        ON  sg.ptid = tp.ptid
        AND sg.stop_seq >= ts.ij_stop_seqs[1]
        AND sg.stop_seq < ts.ij_stop_seqs[2]
    ),
    cumul_times AS (
      SELECT
        ttid,
        segno,
        ij_times,
        ij_seconds * (seg_dist / section_dist)  AS seg_seconds,
        sum(ij_seconds * (seg_dist / section_dist)) OVER (
          PARTITION BY ttid, ij_stop_seqs ORDER BY segno
        ) AS cumul_seg_seconds,
        ij_timepoints
      FROM timepoint_spans_to_segs
    ),
    inserted AS (
      INSERT INTO sched.segment_times (
        ttid, segno, ij_times, ij_timepoints
      )
      SELECT
        ttid,
        segno,
        ARRAY[
          ij_times[1] + make_interval(secs => cumul_seg_seconds - seg_seconds),
          make_interval(secs => cumul_seg_seconds)
        ] AS ij_times,
        ij_timepoints
      FROM cumul_times
      ORDER BY ttid, segno
      RETURNING *
    )
  SELECT 'sched.segment_times' AS table_name, count(*) AS rows_affected
  FROM inserted;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.transfer_templates(text) IS
'Populate `sched.segment_times` from `stage_gtfs` templates table hierarchy.`
Effectively, timepoint stop time pairs in `stage_gtfs.template_stops` are "spanned"
over the network path such that each segment gets enter and exit times interpolated
by the cumulative path distance.
- `where_sql`: NOT IMPLEMENTED YET
- Source tables   `stage_gtfs.template_stops`,
                  `sched.templates`,
                  `sched.segments`
- Target tables:  `sched.segment_times`';
