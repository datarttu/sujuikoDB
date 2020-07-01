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
  -- Movement values are to be calcluated by lag -> current,
  -- except for the first record per jrnid current -> lead
  dx            double precision,
  spd           double precision,
  acc           double precision,
  hdg           double precision
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
    OR NEW.odo    IS NULL
  THEN RETURN NULL;
  ELSE RETURN NEW;
  END IF;
END;
$$;
COMMENT ON FUNCTION stage_hfp.ignore_invalid_raw_rows() IS
'Blocks from insertion any row where any of the conditions apply:
- is_ongoing is not true
- event_type is other than VP
- tst, route, dir, oday, start, oper, veh, lon, lat or odo is null';

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

DROP FUNCTION IF EXISTS stage_hfp.set_obs_nums;
CREATE FUNCTION stage_hfp.set_obs_nums()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
  RAISE NOTICE 'Updating obs_num values ...';
  /*
   * Window functions cannot be used directly in an UPDATE statement
   * but require a self join.
   * Since the target table does not have a reliable primary key for self join,
   * we use PG's system column "ctid" that is included in every table
   * identifying the unique position of each row.
   * Note that ctid values are safe and durable only inside the same transaction.
   */
  EXECUTE format(
    $s$
    WITH rownums AS (
      SELECT
        row_number() OVER (PARTITION BY jrnid ORDER BY tst)  AS obs_num,
        ctid
      FROM %1$I.%2$I
    )
    UPDATE %1$I.%2$I AS upd
    SET obs_num = rn.obs_num
    FROM (SELECT * FROM rownums) AS rn
    WHERE upd.ctid = rn.ctid
    $s$,
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

DROP FUNCTION IF EXISTS stage_hfp.set_movement_values;
CREATE FUNCTION stage_hfp.set_movement_values()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
BEGIN
  RAISE NOTICE 'Updating spd, acc, hdg values ...';
  EXECUTE format(
    $s$
    WITH
    src_points AS (
      SELECT
        ctid,
        jrnid,
        tst,
        CASE
          WHEN obs_num = 1 THEN geom
          ELSE lag(geom) OVER w_tst
        END   AS start_point,
        CASE
          WHEN obs_num = 1 THEN lead(geom) OVER w_tst
          ELSE geom
        END   AS end_point,
        extract (epoch FROM
          CASE
            WHEN obs_num = 1 THEN (lead(tst) OVER w_tst) - tst
            ELSE tst - lag(tst) OVER w_tst
          END
        )     AS delta_time -- in seconds
      FROM %1$I.%2$I
      WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    ),
    hdg_spd AS (
      SELECT
        ctid,
        jrnid,
        tst,
        delta_time,
        degrees(
          ST_Azimuth(start_point, end_point)
        )::double precision                 AS hdg,
        ST_Distance(start_point, end_point) AS dx,
        CASE
          WHEN delta_time = 0.0 THEN NULL
          ELSE ST_Distance(start_point, end_point) / delta_time
        END                                 AS spd
      FROM src_points
    ),
    hdg_spd_acc AS (
      SELECT
        ctid,
        dx,
        hdg,
        spd,
        CASE
          WHEN delta_time = 0.0 THEN NULL
          ELSE (spd - coalesce(lag(spd) OVER w_tst, 0.0)) / delta_time
        END                   AS acc
      FROM hdg_spd
      WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    )
    UPDATE %1$I.%2$I AS upd
    SET
      dx  = hsa.dx,
      spd = hsa.spd,
      acc = hsa.acc,
      hdg = hsa.hdg
    FROM (SELECT * FROM hdg_spd_acc) AS hsa
    WHERE upd.ctid = hsa.ctid
    $s$,
    TG_TABLE_SCHEMA, TG_TABLE_NAME
  );

  RETURN NULL;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_movement_values() IS
'Updates `spd`, `acc` and `hdg` values of the target table
by window functions partitioned by `jrnid` and ordered by `tst`.
Values are calculated from lag to current row,
except for the first of `jrnid` from current to lead row.
This function should be run again if rows have been deleted.';
