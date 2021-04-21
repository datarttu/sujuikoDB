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
  dodo          double precision,
  dx            double precision,
  spd           double precision,
  acc           double precision,
  hdg           double precision
);
COMMENT ON TABLE stage_hfp.raw IS
'Accommodates raw HFP observations read from csv dumps.
Fields from `is_ongoing` up to `route` are expected to be populated
from the csv files. Fields `jrnid`-`geom` are calculated later
using values from the same rows, `obs_num` as running number along `tst`
within each `jrnid` group, `dodo`-`hdg` by window functions based on
preceding or following rows.';

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
CREATE FUNCTION stage_hfp.set_obs_nums(raw_table regclass)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd   bigint;
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
    $s$
    WITH rownums AS (
      SELECT
        row_number() OVER (PARTITION BY jrnid ORDER BY tst)  AS obs_num,
        ctid
      FROM %1$s
    ),
    updated AS (
      UPDATE %1$s AS upd
      SET obs_num = rn.obs_num
      FROM (SELECT * FROM rownums) AS rn
      WHERE upd.ctid = rn.ctid
      RETURNING 1
    )
    SELECT count(*) FROM updated
    $s$,
    raw_table
  ) INTO cnt_upd;
  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_obs_nums IS
'Update `obs_num` field of `raw_table` with a running number from 1
ordered by `tst` for each `jrnid` partition.';

DROP FUNCTION IF EXISTS stage_hfp.set_raw_movement_values;
CREATE FUNCTION stage_hfp.set_raw_movement_values(
  target_table  regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
    src_points AS (
      SELECT
        ctid,
        jrnid,
        tst,
        odo,
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
      FROM %1$s
      WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    ),
    hdg_spd AS (
      SELECT
        ctid,
        jrnid,
        tst,
        odo,
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
        END                     AS acc,
        coalesce(odo - (lag(odo) OVER w_tst),
          0)::double precision  AS dodo
      FROM hdg_spd
      WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    ),
    updated AS (
      UPDATE %1$s AS upd
      SET
        dodo  = hsa.dodo,
        dx    = hsa.dx,
        spd   = hsa.spd,
        acc   = hsa.acc,
        hdg   = hsa.hdg
      FROM (SELECT * FROM hdg_spd_acc) AS hsa
      WHERE upd.ctid = hsa.ctid
      RETURNING *
    )
    SELECT count(*) FROM updated
    $s$,
    target_table
  ) INTO cnt_upd;

  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_raw_movement_values(regclass) IS
'Updates `dodo`, `dx`, `spd`, `acc` and `hdg` of the `target_table`
by window functions partitioned by `jrnid` and ordered by `tst`.
Values are calculated from lag to current row,
except for the first of `jrnid` from current to lead row.
This function should be run again if rows have been deleted.';

DROP FUNCTION IF EXISTS stage_hfp.delete_duplicates_by_tst;
CREATE FUNCTION stage_hfp.delete_duplicates_by_tst(
  target_table  regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_del   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH
    counts AS (
      SELECT
        ctid,
        jrnid,
        tst,
        dodo,
        spd,
        count(*) OVER (PARTITION BY jrnid, tst) AS cnt
      FROM %1$s
    ),
    having_duplicates AS (
      SELECT *
      FROM counts
      WHERE cnt > 1
      ORDER BY dodo DESC, spd ASC NULLS LAST
    ),
    spared AS (
      SELECT DISTINCT ON (jrnid, tst) ctid
      FROM having_duplicates
    ),
    deleted AS (
      DELETE FROM %1$s
      WHERE ctid IN (
        SELECT ctid FROM having_duplicates
        EXCEPT
        SELECT ctid FROM spared
      )
      RETURNING *
    )
    SELECT count(*) FROM deleted;
    $s$,
    target_table
  ) INTO cnt_del;

  RETURN cnt_del;
END;
$$;
COMMENT ON FUNCTION stage_hfp.delete_duplicates_by_tst(regclass) IS
'Deletes from `target_table` rows that are duplicated over `jrnid` and `tst`.
The row left in the table has
1)  maximum `dodo` of the duplicates, i.e. assumed to have most information
    about real movement, and at the same time
2)  minimum non-null `spd`, i.e. least GPS movement, by which we aim to eliminate
    rows with clear GPS error.';

DROP FUNCTION IF EXISTS stage_hfp.drop_with_invalid_movement_values;
CREATE FUNCTION stage_hfp.drop_with_invalid_movement_values(
  target_table  regclass,
  max_spd       numeric,
  min_acc       numeric,
  max_acc       numeric
)
RETURNS TABLE (
  n_deleted     bigint,
  n_updated     bigint
)
LANGUAGE PLPGSQL
AS $$
DECLARE
  this_rec      record;
  prev_rec      record;
  this_jrnid    uuid;
  delta_time    double precision;
  n_deleted     bigint;
  n_updated     bigint;
BEGIN
  -- Storage for our rows to update and delete;
  -- cannot do these operations directly to the table when looping.
  CREATE TEMPORARY TABLE _to_update (
    jrnid   uuid,
    obs_num bigint,
    dodo    double precision,
    dx      double precision,
    spd     double precision,
    acc     double precision,
    hdg     double precision
  ) ON COMMIT DROP;
  CREATE TEMPORARY TABLE _to_delete (
    jrnid   uuid,
    obs_num bigint
  ) ON COMMIT DROP;

  <<mainloop>>
  FOR this_rec IN EXECUTE format(
    $s$SELECT * FROM %1$s ORDER BY jrnid, obs_num$s$,
    target_table
  )
  LOOP
    -- First row of the jrnid partition is always trated as valid
    -- so we do not need to do forward calculations.
    IF this_rec.jrnid IS DISTINCT FROM this_jrnid THEN
      this_jrnid := this_rec.jrnid;
      prev_rec := this_rec;
      --rec_offset := 1;
      CONTINUE mainloop;
    END IF;

    this_rec.dodo := (this_rec.odo - prev_rec.odo);
    this_rec.dx   := ST_Distance(this_rec.geom, prev_rec.geom);
    delta_time    := extract(epoch FROM this_rec.tst - prev_rec.tst);
    this_rec.spd  := CASE
                       WHEN delta_time = 0.0 THEN NULL
                       ELSE this_rec.dx / delta_time
                     END;
    this_rec.acc  := CASE
                       WHEN delta_time = 0.0 THEN NULL
                       ELSE (this_rec.spd - coalesce(prev_rec.spd, 0.0)) / delta_time
                     END;
    this_rec.hdg  := degrees(ST_Azimuth(prev_rec.geom, this_rec.geom));

    IF coalesce(this_rec.spd, 0.0) > max_spd
      OR coalesce(this_rec.acc, 0.0) < min_acc
      OR coalesce(this_rec.acc, 0.0) > max_acc
      THEN
      INSERT INTO _to_delete VALUES (
        this_rec.jrnid, this_rec.obs_num
      );
    ELSE
      INSERT INTO _to_update VALUES (
        this_rec.jrnid, this_rec.obs_num,
        this_rec.dodo, this_rec.dx,
        this_rec.spd, this_rec.acc, this_rec.hdg
      );
      prev_rec  := this_rec;
    END IF;

  END LOOP mainloop;

  EXECUTE format(
    $s$
    WITH updated AS (
      UPDATE %1$s AS upd
      SET
        dodo  = tu.dodo,
        dx    = tu.dx,
        spd   = tu.spd,
        acc   = tu.acc,
        hdg   = tu.hdg
      FROM (SELECT * FROM _to_update) AS tu
      WHERE upd.jrnid = tu.jrnid AND upd.obs_num = tu.obs_num
      RETURNING *
    )
    SELECT count(*) FROM updated
    $s$,
    target_table
  ) INTO n_updated;
  DROP TABLE _to_update;

  EXECUTE format(
    $s$
    WITH deleted AS (
      DELETE FROM %1$s AS del
      USING _to_delete AS td
      WHERE del.jrnid = td.jrnid AND del.obs_num = td.obs_num
      RETURNING *
    )
    SELECT count(*) FROM deleted
    $s$,
    target_table
  ) INTO n_deleted;
  DROP TABLE _to_delete;

  RETURN QUERY SELECT n_deleted, n_updated;
END;
$$;
COMMENT ON FUNCTION stage_hfp.drop_with_invalid_movement_values IS
'Deletes from `target_table` records considered invalid by `spd` and `acc` values,
scanning through the table ordered by `jrnid` and `obs_num` and updating successive
records "on the fly" if records are deleted from between.
- `target_table`: schema-qualified or temporary table name
- `max_spd`:      maximum speed m/s (not km/h!)
- `min_acc`:      minimum acceleration m/s2 (note: should be negative)
- `max_acc`:      maximum acceleration m/s2';
