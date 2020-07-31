CREATE SCHEMA IF NOT EXISTS obs;
COMMENT ON SCHEMA obs IS
'Stores the production-ready data of observed transit journeys
and their movements on network segments.';

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
CREATE INDEX ON obs.journeys USING BTREE (extract(date FROM start_ts AT TIME ZONE 'Europe/Helsinki'));
CREATE INDEX ON obs.journeys USING BTREE (ttid);
CREATE INDEX ON obs.journeys USING BTREE (oper, veh);
/*
CREATE TABLE obs.segments (
  enter_ts   timestamptz      NOT NULL,
  linkid     integer          NOT NULL REFERENCES nw.links(linkid),
  inode      integer          NOT NULL REFERENCES nw.nodes(nodeid),
  jnode      integer          NOT NULL REFERENCES nw.nodes(nodeid),
  jrnid      uuid             NOT NULL REFERENCES obs.journeys(jrnid),
  exit_ts    timestamptz,
  traj_pts   jsonb,
  PRIMARY KEY (enter_ts, linkid, jrnid)
);
*/
