DROP TABLE IF EXISTS stage_hfp.journeys CASCADE;
CREATE TABLE stage_hfp.journeys (
  jrnid             uuid            PRIMARY KEY,
  ttid              text,
  ptid              text,

  start_ts          timestamptz     NOT NULL,
  route             text            NOT NULL,
  dir               smallint        NOT NULL,
  oper              smallint        NOT NULL,
  veh               integer         NOT NULL,

  n_obs             integer,
  n_dropen          integer,
  tst_span          tstzrange,
  odo_span          int4range,
  raw_distance      double precision,

  invalid_reasons   text[]          DEFAULT '{}'
);
COMMENT ON TABLE stage_hfp.journeys IS
'Common values and aggregates of each `jrnid` journey entry
extracted from `stage_hfp.raw` or corresponding temp table.';

DROP FUNCTION IF EXISTS stage_hfp.insert_to_journeys_from_raw;
CREATE OR REPLACE FUNCTION stage_hfp.insert_to_journeys_from_raw()
RETURNS TABLE (table_name text, rows_inserted bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY
  WITH inserted AS (
    INSERT INTO stage_hfp.journeys (
      jrnid, start_ts, route, dir, oper, veh,
      tst_span, n_ongoing, n_odo_values, odo_span,
      n_geom_values, n_door_open, n_door_closed, n_uniq_stops
    )
    SELECT
      jrnid,
      start_ts,
      route,
      dir,
      oper,
      veh,
      tstzrange(min(tst), max(tst))           AS tst_span,
      count(*)                                AS n_ongoing,
      count(*) filter(WHERE odo IS NOT NULL)  AS n_odo_values,
      int4range(min(odo), max(odo))           AS odo_span,
      count(*) filter(WHERE geom IS NOT NULL) AS n_geom_values,
      count(*) filter(WHERE drst IS true)     AS n_door_open,
      count(*) filter(WHERE drst IS false)    AS n_door_closed,
      count(DISTINCT stop) filter(WHERE stop IS NOT NULL) AS n_uniq_stops
    FROM stage_hfp.raw
    WHERE jrnid IS NOT NULL
      AND is_ongoing IS true
    GROUP BY jrnid, start_ts, route, dir, oper, veh
    ORDER BY start_ts
    RETURNING *
  )
  SELECT 'journeys', count(*)
  FROM inserted;
END;
$$;

DROP FUNCTION IF EXISTS stage_hfp.set_journeys_ttid;
CREATE OR REPLACE FUNCTION stage_hfp.set_journeys_ttid()
RETURNS TABLE (table_name text, rows_updated bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY
  WITH updated AS (
    UPDATE stage_hfp.journeys AS jrn
    SET ttid = vt.ttid
    FROM (
      SELECT ttid, start_ts, route, dir
      FROM sched.view_trips
    ) AS vt
    WHERE jrn.start_ts = vt.start_ts
      AND jrn.route = vt.route
      AND jrn.dir = vt.dir
    RETURNING *
  )
  SELECT 'journeys', count(*)
  FROM updated;
END;
$$;
