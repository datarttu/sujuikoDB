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
  geom          geometry(POINT, 3067),
  obs_num       bigint,
  -- speeds[1]: lag -> current, speeds[2]: current -> lead
  speeds        double precision[],
  -- acceleration values: the same way
  accs          double precision[]
);

CREATE INDEX jrnid_tst_idx ON stage_hfp.raw USING BTREE (jrnid, tst);
CREATE INDEX ON stage_hfp.raw USING BTREE (start_ts, route, dir);
CREATE INDEX ON stage_hfp.raw USING GIST (geom);

DROP FUNCTION IF EXISTS stage_hfp.ignore_invalid_raw_rows;
CREATE FUNCTION stage_hfp.ignore_invalid_raw_rows()
RETURNS trigger
LANGUAGE PLPGSQL
AS $$
BEGIN
  IF
    NEW.is_ongoing IS false OR NEW.is_ongoing IS NULL
    OR NEW.event_type <> 'VP' OR NEW.event_type IS NULL
    OR NEW.tst    IS NULL
    OR NEW.route  IS NULL
    OR NEW.dir    IS NULL
    OR NEW.oday   IS NULL
    OR NEW.start  IS NULL
    OR NEW.oper   IS NULL
    OR NEW.veh    IS NULL
    OR NEW.lon    IS NULL
    OR NEW.lat    IS NULL
  THEN RETURN NULL;
  ELSE RETURN NEW;
  END IF;
END;
$$;
COMMENT ON FUNCTION stage_hfp.ignore_invalid_raw_rows() IS
'Blocks from insertion any row where any of the conditions apply:
- is_ongoing is not true
- event_type is other than VP
- tst, route, dir, oday, start, oper, veh, lon or lat is null';

CREATE TRIGGER t10_ignore_invalid_raw_rows
BEFORE INSERT ON stage_hfp.raw
FOR EACH ROW
EXECUTE PROCEDURE stage_hfp.ignore_invalid_raw_rows();

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

CREATE TRIGGER t20_set_raw_additional_fields
BEFORE INSERT ON stage_hfp.raw
FOR EACH ROW
EXECUTE PROCEDURE stage_hfp.set_raw_additional_fields();

DROP FUNCTION IF EXISTS stage_hfp.set_obs_nums;
CREATE FUNCTION stage_hfp.set_obs_nums()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
  /*
   * Window functions cannot be used directly in an UPDATE statement
   * but require a self join.
   * Since the target table does not have a reliable primary key for self join,
   * we use PG's system column "ctid" that is included in every table
   * identifying the unique position of each row.
   * Note that ctid values are safe and durable only inside the same transaction.
   */
  EXECUTE format(
    'WITH rownums AS (
      SELECT
        row_number() OVER (PARTITION BY jrnid ORDER BY tst)  AS obs_num,
        ctid
      FROM %1$I.%2$I
    )
    UPDATE %1$I.%2$I AS upd
    SET obs_num = rn.obs_num
    FROM (SELECT * FROM rownums) AS rn
    WHERE upd.ctid = rn.ctid',
    TG_TABLE_SCHEMA, TG_TABLE_NAME
  );
  RETURN NULL;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_obs_nums() IS
'Updates the `obs_num` field of the target table with a running number from 1
ordered by `tst` for each `jrnid` partition.
Since this is a trigger function, target schema and table are automatically
resolved by `TG_TABLE_SCHEMA` and `TG_TABLE_NAME`.';

CREATE TRIGGER t30_set_obs_nums
AFTER INSERT ON stage_hfp.raw
FOR EACH STATEMENT
EXECUTE PROCEDURE stage_hfp.set_obs_nums();
