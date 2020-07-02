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

DROP FUNCTION IF EXISTS stage_hfp.extract_journeys_from_raw;
CREATE OR REPLACE FUNCTION stage_hfp.extract_journeys_from_raw(
  raw_table       regclass,
  journey_table   regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_ins   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH inserted AS (
      INSERT INTO %1$s (
        jrnid, start_ts, route, dir, oper, veh,
        n_obs, n_dropen, tst_span, odo_span, raw_distance
      )
      SELECT
        jrnid,
        start_ts,
        route,
        dir,
        oper,
        veh,
        count(*)                                AS n_obs,
        count(*) filter(WHERE drst IS true)     AS n_dropen,
        tstzrange(min(tst), max(tst))           AS tst_span,
        int4range(min(odo), max(odo))           AS odo_span,
        sum(dx)                                 AS raw_distance
      FROM %2$s
      GROUP BY jrnid, start_ts, route, dir, oper, veh
      ORDER BY start_ts
      RETURNING *
    )
    SELECT count(*) FROM inserted
    $s$,
    journey_table,
    raw_table
  ) INTO cnt_ins;

  RETURN cnt_ins;
END;
$$;
COMMENT ON FUNCTION stage_hfp.extract_journeys_from_raw IS
'Extracts common attributes of each journey `jrnid` as well as
aggregate values of journeys from raw HFP table `raw_table`
and inserts the results into journey table `journey_table`.';
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
