CREATE TABLE stage_gtfs.stops_with_mode (
  stopid        integer                   PRIMARY KEY,
  mode          public.mode_type          NOT NULL,
  code          text,
  name          text,
  descr         text,
  parent        integer,
  geom          geometry(POINT, 3067)     NOT NULL,
  geom_history  geometry(POINT, 3067)[],
  history_times timestamptz[]
);
COMMENT ON TABLE stage_gtfs.stops_with_mode IS
'Stops with travel mode.
History fields describe old geometries and the times they were modified.';
CREATE INDEX stops_with_mode_geom_idx
  ON stage_gtfs.stops_with_mode
  USING GIST(geom);

CREATE FUNCTION stage_gtfs.record_geom_mods()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
  IF NEW.geom IS DISTINCT FROM OLD.geom THEN
    NEW.history_times = array_append(OLD.history_times, now());
    NEW.geom_history = array_append(OLD.geom_history, OLD.geom);
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER geom_mod_recorder
  BEFORE UPDATE ON stage_gtfs.stops_with_mode
  FOR EACH ROW
  EXECUTE FUNCTION stage_gtfs.record_geom_mods();

CREATE OR REPLACE FUNCTION stage_gtfs.populate_stops_with_mode()
RETURNS TABLE (
  mode            public.mode_type,
  rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RETURN QUERY
  WITH
    mode_trips AS (
      SELECT t.trip_id, r.mode
      FROM stage_gtfs.trips     AS t
      INNER JOIN sched.routes   AS r
        ON t.route_id = r.route
    ),
    mode_stoptimes AS (
      SELECT DISTINCT st.stop_id, t.mode
      FROM stage_gtfs.stop_times  AS st
      INNER JOIN mode_trips       AS t
      ON st.trip_id = t.trip_id
    ),
    inserted AS (
      INSERT INTO stage_gtfs.stops_with_mode (
        stopid, mode, code, name, descr, parent, geom
      )
      SELECT
        s.stop_id     AS stopid,
        m.mode        AS mode,
        s.stop_code   AS code,
        s.stop_name   AS name,
        s.stop_desc   AS desc,
        s.parent_station	AS parent,
        ST_Transform(
          ST_SetSRID(
            ST_MakePoint(s.stop_lon, s.stop_lat), 4326),
          3067) AS geom
      FROM stage_gtfs.stops     AS s
      INNER JOIN mode_stoptimes AS m
      ON s.stop_id = m.stop_id
      ORDER BY s.stop_id, m.mode
      ON CONFLICT DO NOTHING
      RETURNING *
    )
  SELECT i.mode, count(i.stopid)::bigint AS rows_inserted
  FROM inserted AS i
  GROUP BY i.mode
  ORDER BY i.mode;
END;
$$;
