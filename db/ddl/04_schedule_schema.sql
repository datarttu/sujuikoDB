/*
 * Create tables for the scheduled (planned) transit service schema.
 *
 * Depends on the network schema "nw".
 *
 * Arttu K 2020-02
 */
\c sujuiko;

CREATE SCHEMA IF NOT EXISTS sched;

/*
 * "timing" refers to the quality of a scheduled timestamp at a node or on a link:
 *
 * - `strict`:   strictly scheduled dep / arr timepoint at a stop in schedule
 * - `approx`:   estimated dep / arr time at a non-timepoint stop in schedule
 * - `interp`:   timestamp has been linearly interpolated (i.e., no schedule at hand for that location)
 */
CREATE TYPE sched.timing_type AS ENUM('strict', 'approx', 'interp');

CREATE TABLE sched.services (
  serv       text              NOT NULL,
  date       date              NOT NULL,
  PRIMARY KEY (serv, date)
);

CREATE TABLE sched.routes (
  route      text              PRIMARY KEY,
  mode       public.mode_type  NOT NULL
);

CREATE TABLE sched.trips (
  tripid     text              PRIMARY KEY,
  serv       text              NOT NULL REFERENCES sched.services(serv),
  route      text              NOT NULL REFERENCES sched.routes(route),
  dir        smallint          NOT NULL
);
CREATE INDEX trips_serv_idx
  ON sched.trips (serv);
CREATE INDEX trips_route_dir_idx
  ON sched.trips (route, dir);

CREATE TABLE sched.segments (
  tripid         text                NOT NULL REFERENCES sched.trips(tripid),
  inode          integer             NOT NULL,
  jnode          integer             NOT NULL,
  enter_hms      interval            NOT NULL,
  exit_hms       interval,
  enter_timing   sched.timing_type,
  exit_timing    sched.timing_type,
  PRIMARY KEY (tripid, inode, jnode, enter_hms),
  FOREIGN KEY (inode, jnode) REFERENCES nw.links(inode, jnode)
);
