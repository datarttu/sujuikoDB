DROP TABLE IF EXISTS stage_hfp.journeys CASCADE;
CREATE TABLE stage_hfp.journeys (
  -- Fields calculated immediately when inserting
  jrnid             uuid        PRIMARY KEY,
  start_ts          timestamptz NOT NULL,
  route             text        NOT NULL,
  dir               smallint    NOT NULL,
  oper              smallint    NOT NULL,
  veh               integer     NOT NULL,

  tst_span          tstzrange,
  n_ongoing         integer,
  n_odo_values      integer,
  odo_span          int4range,
  n_geom_values     integer,
  n_door_open       integer,
  n_door_closed     integer,
  n_uniq_stops      smallint,

  -- Calculated using sched.view_trips
  ttid              text,

  -- Calculated from .journey_points if ttid was not null
  line_raw_length   real,
  line_tt_length    real,
  line_ref_length   real,
  ref_avg_dist      real,
  ref_med_dist      real,
  ref_max_dist      real,
  ref_n_within      integer,

  actual_start_ts   timestamptz,

  invalid_reasons   text[]      DEFAULT '{}'
);

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
