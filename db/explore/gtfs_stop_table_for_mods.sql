/*
 * Create a version of GTFS stops in staging schema
 * where geometries can be modified
 * and the modifications are tracked by a trigger.
 */

BEGIN;

CREATE TABLE stage_gtfs.stops_for_modification (
  stopid        integer                 PRIMARY KEY,
  mode          public.mode_type,
  code          text,
  name          text,
  descr         text,
  parent        integer,
  geom          geometry(POINT, 3067),
  geom_history  geometry(POINT, 3067)[],
  history_times timestamptz[]
);

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
  BEFORE UPDATE ON stage_gtfs.stops_for_modification
  FOR EACH ROW
  EXECUTE FUNCTION stage_gtfs.record_geom_mods();

INSERT INTO stage_gtfs.stops_for_modification (stopid, geom)
VALUES (1, ST_SetSRID(ST_MakePoint(0, 0), 3067));

SELECT stopid, unnest(geom_history), unnest(history_times)
FROM stage_gtfs.stops_for_modification;

UPDATE stage_gtfs.stops_for_modification
SET geom = ST_SetSRID(ST_MakePoint(1, 1), 3067);

SELECT stopid, unnest(geom_history), unnest(history_times)
FROM stage_gtfs.stops_for_modification;

UPDATE stage_gtfs.stops_for_modification
SET geom = ST_SetSRID(ST_MakePoint(1, 1), 3067);

SELECT stopid, unnest(geom_history), unnest(history_times)
FROM stage_gtfs.stops_for_modification;

SELECT pg_sleep(2);

UPDATE stage_gtfs.stops_for_modification
SET geom = ST_SetSRID(ST_MakePoint(1, 2), 3067);

SELECT stopid, unnest(geom_history), unnest(history_times)
FROM stage_gtfs.stops_for_modification;

ROLLBACK;
