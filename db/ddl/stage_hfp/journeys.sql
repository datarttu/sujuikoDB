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
  n_neg_dodo        integer,

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
        n_obs, n_dropen, tst_span, odo_span, raw_distance, n_neg_dodo
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
        sum(dx)                                 AS raw_distance,
        count(*) filter(WHERE dodo < 0)         AS n_neg_dodo
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

DROP FUNCTION IF EXISTS stage_hfp.set_journeys_ttid_ptid;
CREATE OR REPLACE FUNCTION stage_hfp.set_journeys_ttid_ptid(
  target_table    regclass
)
RETURNS BIGINT
VOLATILE
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_upd   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH updated AS (
      UPDATE %1$s AS jrn
      SET
        ptid = vt.ptid,
        ttid = vt.ttid
      FROM (
        SELECT ptid, ttid, start_ts, route, dir
        FROM sched.view_trips
      ) AS vt
      WHERE jrn.start_ts = vt.start_ts
        AND jrn.route = vt.route
        AND jrn.dir = vt.dir
      RETURNING *
    )
    SELECT count(*) FROM updated
    $s$,
    target_table
  ) INTO cnt_upd;

  RETURN cnt_upd;
END;
$$;
COMMENT ON FUNCTION stage_hfp.set_journeys_ttid_ptid IS
'Look up template and pattern ids from sched schema,
based on journey start timestamp, route and direction,
and set their values in `target_table`.';

DROP FUNCTION IF EXISTS stage_hfp.discard_invalid_journeys;
CREATE OR REPLACE FUNCTION stage_hfp.discard_invalid_journeys(
  target_table      regclass,
  log_to_discarded  boolean   DEFAULT true
)
RETURNS bigint
VOLATILE
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_del   bigint;
  del_sql   text;
  end_sql   text;
BEGIN
  del_sql :=  format(
    $s$
    WITH deleted AS (
      DELETE FROM %1$s
      WHERE cardinality(invalid_reasons) > 0
      RETURNING *
    )
    $s$,
    target_table
  );
  IF log_to_discarded IS true THEN
    end_sql := $s$
    ,
    logged AS (
      INSERT INTO stage_hfp.discarded_journeys
      SELECT *, now()
      FROM deleted
      RETURNING *
    )
    SELECT count(*) FROM logged
    $s$;
  ELSE
    end_sql := $s$
      SELECT count(*) FROM deleted
    $s$;
  END IF;

  EXECUTE del_sql || end_sql INTO cnt_del;

  RETURN cnt_del;
END;
$$;
COMMENT ON FUNCTION stage_hfp.discard_invalid_journeys IS
'Delete from `target_table` rows with any `invalid_reasons` messages.
If `log_to_discarded` is true (default), save the deleted rows
into `stage_hfp.discarded_journeys`.';
