DROP TABLE IF EXISTS stage_hfp.journeys CASCADE;
DROP FUNCTION IF EXISTS stage_hfp.insert_to_journeys_from_raw;

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

CREATE INDEX ON stage_hfp.journeys USING BRIN(start_ts);
CREATE INDEX ON stage_hfp.journeys (route, dir);
CREATE INDEX ON stage_hfp.journeys (oper, veh);
CREATE INDEX ON stage_hfp.journeys (array_length(invalid_reasons, 1));

CREATE OR REPLACE FUNCTION stage_hfp.insert_to_journeys_from_raw()
RETURNS TEXT
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
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
  WHERE is_ongoing IS true
  GROUP BY jrnid, start_ts, route, dir, oper, veh
  ORDER BY start_ts;

  RETURN 'OK';
END;
$$;

/*
CREATE TABLE stage_hfp.journey_points (
  jrnid             uuid                  NOT NULL REFERENCES stage_hfp.journeys(jrnid),
  obs_num           integer               NOT NULL,
  tst               timestamptz           NOT NULL,
  event             public.event_type     NOT NULL,
  odo               integer,
  drst              boolean,
  stop              integer,

  -- Reference link based values (calculate by joining the closest corresponding trip template segment)
  ref_linkid        integer,
  ref_reversed      boolean,
  ref_offset        real,     -- Closest distance to the reference link
  ref_loc           real,     -- Location of closest point on ref link, 0 ... link length

  geom_orig         geometry(POINT, 3067),
  geom_ref          geometry(POINT, 3067),

  PRIMARY KEY (jrnid, obs_num)
);
*/
