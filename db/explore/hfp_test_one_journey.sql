BEGIN;

DROP SCHEMA IF EXISTS test_jrn CASCADE;
CREATE SCHEMA test_jrn;

CREATE TABLE test_jrn.journeys (
  jrnid             uuid        PRIMARY KEY,
  oday              date        NOT NULL,
  start             interval    NOT NULL,
  route             text        NOT NULL,
  dir               smallint    NOT NULL CHECK (dir IN (1, 2)),
  oper              smallint    NOT NULL,
  veh               integer     NOT NULL,

  n_total           integer,
  n_ongoing         integer,
  n_dooropen        integer,
  tst_span          tstzrange,

  ttid              text,

  line_raw_length   real,
  line_tt_length    real,
  line_ref_length   real,
  ref_avg_dist      real,
  ref_med_dist      real,
  ref_max_dist      real,
  ref_n_accept      integer,

  invalid_reasons   text[]      DEFAULT '{}'
);

WITH jrn AS (
  SELECT
    jrnid,
    min(oday) AS oday,
    min(start) AS start,
    min(route) AS route,
    min(dir) AS dir,
    min(oper) AS oper,
    min(veh) AS veh
  FROM stage_hfp.raw
  WHERE jrnid = '9dd55eec-2117-9e97-8cfd-95ed9f19239d'
  GROUP BY jrnid
)
INSERT INTO test_jrn.journeys (
  jrnid, oday, start, route, dir, oper, veh, ttid
)
SELECT
  jrn.*,
  it.ttid
FROM jrn
INNER JOIN sched.individual_trips AS it
  ON jrn.route = it.route
  AND jrn.dir = it.dir
  AND jrn.oday = it.service_date
  AND jrn.start = it.start_time;

CREATE TABLE test_jrn.ref_links AS (
  SELECT DISTINCT ON (seg.linkid)
    jrn.ttid,
    seg.linkid,
    lin.geom
  FROM test_jrn.journeys      AS jrn
  INNER JOIN sched.segments   AS seg
    ON jrn.ttid = seg.ttid
  INNER JOIN nw.links         AS lin
    ON seg.linkid = lin.linkid
);

CREATE TABLE test_jrn.points_ongoing (
  jrnid             uuid                    NOT NULL,
  jrn_row           integer                 NOT NULL,
  event_type        public.event_type       NOT NULL,
  tst               timestamptz             NOT NULL,
  drst              boolean,
  geom              geometry(POINT, 3067)   NOT NULL,

  ref_linkid        integer,
  ref_geom          geometry(POINT, 3067),
  ref_dist          real,

  tdif_forw         interval,
  dist_forw         real,
  tdif_back         interval,
  dist_back         real,
  speed_kmh         real,

  keep              boolean,

  PRIMARY KEY (jrnid, jrn_row)
);

INSERT INTO test_jrn.points_ongoing (
  jrnid, jrn_row, event_type, tst, drst, geom
)
SELECT
  r.jrnid,
  row_number() OVER (PARTITION BY r.jrnid ORDER BY tst, event_type) AS jrn_row,
  event_type::public.event_type,
  tst,
  drst,
  ST_Transform(
    ST_SetSRID(
      ST_MakePoint(lon, lat),
      4326
    ),
    3067
  ) AS geom
FROM stage_hfp.raw AS r
INNER JOIN test_jrn.journeys AS j
ON r.jrnid = j.jrnid
WHERE is_ongoing IS true
  AND lon IS NOT NULL
  AND lat IS NOT NULL;

-- TODO: Update N values in journeys
-- TODO: Update raw line length in journeys
