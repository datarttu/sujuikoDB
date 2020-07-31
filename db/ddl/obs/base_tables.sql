CREATE SCHEMA IF NOT EXISTS obs;
COMMENT ON SCHEMA obs IS
'Stores the production-ready data of observed transit journeys
and their movements on network segments.';

CREATE TABLE obs.journeys (
  start_ts   timestamptz      NOT NULL,
  ttid       text             NOT NULL REFERENCES sched.templates(ttid),
  jrnid      uuid             NOT NULL, -- TODO: md5 trigger
  vehid      integer          NOT NULL REFERENCES obs.vehicles(vehid),
  PRIMARY KEY (start_ts, ttid)
);
CREATE UNIQUE INDEX journeys_jrnid_idx
  ON obs.journeys (jrnid);
CREATE INDEX journeys_vehid_idx
  ON obs.journeys (vehid);

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
