CREATE SCHEMA obs;

-- JOURNEYS (jrn)
CREATE TABLE obs.journey (
  jrnid           uuid          PRIMARY KEY,
  route           text          NOT NULL,
  dir             smallint      NOT NULL CHECK (dir IN (1, 2)),
  start_tst       timestamptz   NOT NULL,
  route_ver_id    text          NOT NULL REFERENCES nw.route_version(route_ver_id),
  oper            integer       NOT NULL,
  veh             integer       NOT NULL
);

CREATE FUNCTION obs.tg_insert_journey_handler()
RETURNS trigger
AS $$
DECLARE
  correct_jrnid   uuid;
  rtver_id_found  text;
BEGIN
  -- Check that jrnid is calculated correctly
  correct_jrnid := md5(
    concat_ws('_',
      NEW.route,
      NEW.dir,
      (NEW.start_tst AT TIME ZONE 'Europe/Helsinki')::date,
      (NEW.start_tst AT TIME ZONE 'Europe/Helsinki')::time,
      NEW.oper,
      NEW.veh
    )
  )::uuid;
  IF NEW.jrnid <> correct_jrnid THEN
    RAISE NOTICE 'Skipping jrnid %: jrnid should be %', NEW.jrnid, correct_jrnid;
    RETURN NULL;
  END IF;

  -- Check and add route version id
  SELECT INTO rtver_id_found rv.route_ver_id
  FROM nw.route_version AS rv
  WHERE NEW.route = rv.route
    AND NEW.dir = rv.dir
    AND (NEW.start_tst AT TIME ZONE 'Europe/Helsinki')::date <@ rv.valid_during;

  IF NOT FOUND THEN
    RAISE NOTICE 'Skipping jrnid %: route version not found', NEW.jrnid;
    RETURN NULL;
  END IF;

  NEW.route_ver_id := rtver_id_found;

  RETURN NEW;

END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER tg_insert_journey
BEFORE INSERT OR UPDATE ON obs.journey
FOR EACH ROW EXECUTE FUNCTION obs.tg_insert_journey_handler();
