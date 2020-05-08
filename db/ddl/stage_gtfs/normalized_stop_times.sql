CREATE TABLE stage_gtfs.normalized_stop_times (
  trip_id               text,
  arr_time_diff         interval,
  dep_time_diff         interval,
  stop_id               integer,
  stop_sequence         smallint,
  rel_dist_traveled     double precision,
  timepoint             boolean,
  PRIMARY KEY (trip_id, stop_sequence)
);
CREATE INDEX ON stage_gtfs.normalized_stop_times(stop_id);
COMMENT ON TABLE stage_gtfs.normalized_stop_times IS
'Stop times of bus and tram trips, where each trip id is assigned
the initial departure time of the trip, and each stop event
is assigned an arrival and a departure time difference based on that start time.
Also shape_dist_traveled (kilometers) is made into "relative" distance traveled,
meaning at each stop the proportion of the total trip shape length.
This is an intermediate step before the trips are grouped into
records with date & initial departure time arrays
based on route, direction, stop sequence and time difference information.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_normalized_stop_times()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       bigint;
  cnt_fail  bigint;
BEGIN
  DELETE FROM stage_gtfs.normalized_stop_times;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.normalized_stop_times', cnt;

  INSERT INTO stage_gtfs.normalized_stop_times (
    trip_id,
    arr_time_diff,
    dep_time_diff,
    stop_id,
    stop_sequence,
    rel_dist_traveled,
    timepoint
  )
  SELECT
    st.trip_id,
    st.arrival_time - twd.trip_start_hms        AS arr_time_diff,
    st.departure_time - twd.trip_start_hms      AS dep_time_diff,
    st.stop_id,
    st.stop_sequence,
    st.shape_dist_traveled / sl.gtfs_dist_total AS rel_dist_traveled,
    st.timepoint
  FROM stage_gtfs.stop_times AS st
  /*
   * Should there be any non-matching records,
   * left join will leave time differences NULL for us to find later.
   */
  LEFT JOIN stage_gtfs.trips_with_dates AS twd
  ON st.trip_id = twd.trip_id
  LEFT JOIN stage_gtfs.shape_lines      AS sl
  ON twd.shape_id = sl.shape_id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% records inserted into stage_gtfs.normalized_stop_times', cnt;

  SELECT INTO cnt_fail count(*)
  FROM stage_gtfs.normalized_stop_times
  WHERE arr_time_diff < interval '0 seconds' OR arr_time_diff IS NULL
    OR  dep_time_diff < interval '0 seconds' OR dep_time_diff IS NULL;
  IF cnt_fail > 0 THEN
    RAISE WARNING '% records with negative or NULL dep / arr time diffs in stage_gtfs.normalized_stop_times',
     cnt_fail;
  ELSE
    RAISE NOTICE 'All dep / arr differences in stage_gtfs.normalized_stop_times are valid';
  END IF;

  RETURN 'OK';
END;
$$;
