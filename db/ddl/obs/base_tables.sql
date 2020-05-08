/*
 * "vehid" is in fact redundant,
 * as it is directly derived from "oper" and "veh",
 * but we want to use a single primary key
 * to enable simpler joins on vehicles.
 *
 * For now, "vehicles" is under "obs" schema,
 * because we actually import the vehicle data from
 * what we see in HFP observations;
 * there is currently no separate data source for vehicles.
 */
CREATE TABLE obs.vehicles (
  vehid      serial           PRIMARY KEY,
  oper       smallint         NOT NULL,
  veh        integer          NOT NULL
);
CREATE UNIQUE INDEX vehicles_oper_veh_idx
  ON obs.vehicles (oper, veh);

/*
 * Again, "jrnid" is directly dependent on "(tripid, start_ts)",
 * but we want to avoid using composite foreign keys in the
 * child table "obs.segments" that will store huge amounts of data.
 *
 * **TODO:** Test using composite keys anyway?
 *           Or joining segments on md5 of start_ts and tripid on the fly?
 */
CREATE TABLE obs.journeys (
  start_ts   timestamptz      NOT NULL,
  ttid       text             NOT NULL REFERENCES sched.trip_templates(ttid),
  jrnid      uuid             NOT NULL, -- TODO: md5 trigger
  vehid      integer          NOT NULL REFERENCES obs.vehicles(vehid),
  PRIMARY KEY (start_ts, tripid)
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
