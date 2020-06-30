DROP TABLE IF EXISTS stage_hfp.raw;
CREATE TABLE stage_hfp.raw (
  is_ongoing    boolean,
  event_type    text,
  dir           smallint,
  oper          smallint,
  veh           integer,
  tst           timestamptz   NOT NULL,
  lat           double precision,
  lon           double precision,
  odo           integer,
  drst          boolean,
  oday          date,
  start         interval,
  loc           text,
  stop          integer,
  route         text,
  jrnid         uuid,
  start_ts      timestamptz,
  geom          geometry(POINT, 3067)
);

CREATE INDEX ON stage_hfp.raw USING BTREE (jrnid, tst);
CREATE INDEX ON stage_hfp.raw USING BTREE (start_ts, route, dir);
CREATE INDEX ON stage_hfp.raw USING GIST (geom);

DROP FUNCTION IF EXISTS stage_hfp.ignore_inserts;
CREATE FUNCTION stage_hfp.ignore_inserts()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
BEGIN
  IF
    NEW.is_ongoing IS true
    AND NEW.event_type = 'VP'
  THEN RETURN NEW;
  ELSE RETURN NULL;
  END IF;
END;
$$;
COMMENT ON FUNCTION stage_hfp.ignore_inserts() IS
'Blocks from insertion any row where is_ongoing is not true and / or
event_type is other than VP.';

CREATE TRIGGER aaa_ignore_inserts
BEFORE INSERT ON stage_hfp.raw
FOR EACH ROW
EXECUTE PROCEDURE stage_hfp.ignore_inserts();

DROP FUNCTION IF EXISTS stage_hfp.set_raw_additional_fields;
CREATE FUNCTION stage_hfp.set_raw_additional_fields()
RETURNS trigger
LANGUAGE PLPGSQL
AS
$$
BEGIN
  NEW.geom := ST_Transform(
    ST_SetSRID(
      ST_MakePoint(NEW.lon, NEW.lat),
      4326),
    3067);

  NEW.start_ts := (NEW.oday || ' Europe/Helsinki')::timestamptz + NEW.start;

  NEW.jrnid := md5(
    concat_ws(
      '_',
      NEW.start_ts, NEW.route, NEW.dir, NEW.oper, NEW.veh
    )
  )::uuid;

  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_raw_additional_fields() IS
'On insert, calculates the following fields based on source values on the row:
- `geom`
- `start_ts`
- `jrnid`';

CREATE TRIGGER aa_fill_additional_fields
BEFORE INSERT ON stage_hfp.raw
FOR EACH ROW
EXECUTE PROCEDURE stage_hfp.set_raw_additional_fields();
