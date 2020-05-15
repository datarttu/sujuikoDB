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
  jrnid         uuid
);

CREATE INDEX ON stage_hfp.raw USING BTREE (route, dir);
CREATE INDEX ON stage_hfp.raw USING BRIN (oday, start);
CREATE INDEX ON stage_hfp.raw USING BTREE (is_ongoing);
CREATE INDEX ON stage_hfp.raw USING BTREE (jrnid);

CREATE FUNCTION stage_hfp.jrnid_generator()
RETURNS trigger
LANGUAGE PLPGSQL
AS
$$
BEGIN
  NEW.jrnid := md5(
    concat_ws(
      '_',
      NEW.oday,
      NEW.start,
      NEW.route,
      NEW.dir,
      NEW.oper,
      NEW.veh
    )
  )::uuid;
  RETURN NEW;
END;
$$;

CREATE TRIGGER generate_jrnid
BEFORE INSERT ON stage_hfp.raw
FOR EACH ROW
EXECUTE PROCEDURE stage_hfp.jrnid_generator();

SELECT *
FROM create_hypertable('stage_hfp.raw', 'tst', chunk_time_interval => interval '1 hour');