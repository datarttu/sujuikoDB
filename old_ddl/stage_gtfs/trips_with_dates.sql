CREATE TABLE stage_gtfs.trips_with_dates (
  trip_id               text              PRIMARY KEY,
  service_id            text,
  route_id              text,
  direction_id          smallint,
  trip_start_hms        interval,
  shape_id              text,
  dates                 date[]
);
COMMENT ON TABLE stage_gtfs.trips_with_dates IS
'Trips with validity dates included,
without need to join gtfs service date tables.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_trips_with_dates()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       integer;
BEGIN
  DELETE FROM stage_gtfs.trips_with_dates;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.trips_with_dates', cnt;

  INSERT INTO stage_gtfs.trips_with_dates (
    trip_id, service_id, route_id, direction_id, shape_id
  )
  SELECT trip_id, service_id, route_id, direction_id, shape_id
  FROM stage_gtfs.trips;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into stage_gtfs.trips_with_dates', cnt;

  UPDATE stage_gtfs.trips_with_dates AS tr
  SET trip_start_hms = st.departure_time
  FROM (
    SELECT trip_id, departure_time
    FROM stage_gtfs.stop_times
    WHERE stop_sequence = 1
  ) AS st
  WHERE tr.trip_id = st.trip_id;
  RAISE NOTICE 'trip_start_hms field updated for % rows', cnt;

  UPDATE stage_gtfs.trips_with_dates AS tr
  SET
    service_id = sd.service_id,
    dates      = sd.dates
  FROM (
    SELECT service_id, dates
    FROM stage_gtfs.service_dates
  ) AS sd
  WHERE tr.service_id = sd.service_id;
  RAISE NOTICE 'service_id and dates fields updated for % rows', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_gtfs.populate_trips_with_dates IS
'Initializes and fills trips_with_dates table with trip ids,
service ids, trip start times and validity dates.';
