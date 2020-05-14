CREATE TABLE stage_gtfs.successive_stops (
  i_stop        integer           NOT NULL,
  j_stop        integer           NOT NULL,
  PRIMARY KEY (i_stop, j_stop)
);
COMMENT ON TABLE stage_gtfs.successive_stops IS
'Stop pairs that occur in schedules,
for finding network routes between stops.
Based on ALL bus and tram stops found in the GTFS data.';

-- TODO: Use trip template arrays rather than raw stop times
CREATE OR REPLACE FUNCTION stage_gtfs.populate_successive_stops()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM stage_gtfs.successive_stops;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.successive_stops', cnt;

  INSERT INTO stage_gtfs.successive_stops
  SELECT DISTINCT
    a.stop_id   AS i_stop,
    b.stop_id   AS j_stop
  FROM stage_gtfs.stop_times            AS a
  INNER JOIN stage_gtfs.stop_times      AS b
    ON  a.trip_id       = b.trip_id
    AND a.stop_sequence = (b.stop_sequence - 1);
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% stop pairs inserted into stage_gtfs.successive_stops', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_gtfs.populate_successive_stops IS
'Find unique successive stop pairs
from tram & bus stop times
and insert them into stage_gtfs.successive_stops table.
stage_gtfs.successive_stops is emptied first.';
