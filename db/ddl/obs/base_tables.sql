CREATE SCHEMA IF NOT EXISTS obs;
COMMENT ON SCHEMA obs IS
'Stores the production-ready data of observed transit journeys
and their movements on network segments.';

DROP TABLE IF EXISTS obs.journeys CASCADE;
CREATE TABLE obs.journeys (
  jrnid         uuid            PRIMARY KEY,
  start_ts      timestamptz     NOT NULL,
  ttid          text            NOT NULL REFERENCES sched.templates(ttid),
  oper          smallint        NOT NULL,
  veh           integer         NOT NULL,

  n_obs         integer         NOT NULL,
  n_dropen      integer         NOT NULL,
  tst_span      tstzrange       NOT NULL,
  odo_span      int4range       NOT NULL,
  raw_distance  real            NOT NULL
);
COMMENT ON TABLE obs.journeys IS
'Common values and aggregates of unique observed transit trips driven by a unique vehicle.
A scheduled trip (`ttid+start_ts`) can sometimes have multiple realizations with different vehicles.
Fields `n_obs ... raw_distance` are based on import process in `stage_hfp` schema, and they
are included for possible auditing after import.';

CREATE INDEX ON obs.journeys USING BTREE (start_ts);
CREATE INDEX ON obs.journeys USING BTREE (cast(start_ts AT TIME ZONE 'Europe/Helsinki' AS date));
CREATE INDEX ON obs.journeys USING BTREE (ttid);
CREATE INDEX ON obs.journeys USING BTREE (oper, veh);

DROP TABLE IF EXISTS obs.segs CASCADE;
CREATE TABLE obs.segs (
  jrnid             uuid              NOT NULL REFERENCES obs.journeys(jrnid),
  enter_ts          timestamptz       NOT NULL,
  exit_ts           timestamptz       NOT NULL,
  -- We could have a fkey constraint on segno -> sched.segments
  -- and linkid, reversed -> nw.links, but we omit it since this table holds a vast
  -- number of rows and checking the constraints would take a lot of time.
  segno             smallint          NOT NULL,
  linkid            integer           NOT NULL,
  reversed          boolean           NOT NULL,
  -- 0: normal, 1: is first seg, 2: is last seg, 3: is first and last seg of the journey.
  end_segment       smallint CHECK (end_segment IN (0, 1, 2, 3)),
  n                 smallint          NOT NULL,
  n_halts           smallint,
  thru_s            real,
  halted_s          real,
  door_s            real,

  pt_timediffs_s    real[]            DEFAULT '{}',
  pt_seg_locs_m     real[]            DEFAULT '{}',
  pt_speeds_m_s     real[]            DEFAULT '{}',
  pt_doors          boolean[]         DEFAULT '{}',
  pt_obs_nums       integer[]         DEFAULT '{}',
  pt_raw_offsets_m  real[]            DEFAULT '{}',
  pt_halt_offsets_m real[]            DEFAULT '{}',

  PRIMARY KEY (enter_ts, jrnid)
);
COMMENT ON TABLE obs.segs IS
'Vehicle movements of journeys `jrnid` modeled on network segments.
Individual observation points are accessible via the `pt_` array fields
that are supposed to be of the same length on each row.
This table is partitioned by `enter_ts` as a Timescale hypertable.';

SELECT * FROM create_hypertable(
  'obs.segs',
  'enter_ts',
  chunk_time_interval => interval '3 hours',
  if_not_exists       => true
);

CREATE INDEX ON obs.segs USING BTREE (extract(hour FROM enter_ts AT TIME ZONE 'Europe/Helsinki'));
CREATE INDEX ON obs.segs USING BTREE (linkid, reversed);
CREATE INDEX ON obs.segs USING BTREE (n) WHERE n > 0;
