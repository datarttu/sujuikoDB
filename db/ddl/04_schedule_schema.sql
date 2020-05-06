/*
 * Create tables for the scheduled (planned) transit service schema.
 *
 * Depends on the network schema "nw".
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\c sujuiko;

BEGIN;
CREATE SCHEMA IF NOT EXISTS sched;

/*
 * "timing" refers to the quality of a scheduled timestamp at a node or on a link:
 *
 * - `strict`:   strictly scheduled dep / arr timepoint at a stop in schedule
 * - `approx`:   estimated dep / arr time at a non-timepoint stop in schedule
 * - `interp`:   timestamp has been linearly interpolated (i.e., no schedule at hand for that location)
 */
CREATE TYPE sched.timing_type AS ENUM('strict', 'approx', 'interp');

CREATE TABLE sched.routes (
  route      text              PRIMARY KEY,
  mode       public.mode_type  NOT NULL
);

CREATE TABLE sched.trip_templates (
  ttid        text              PRIMARY KEY,
  route       text              NOT NULL REFERENCES sched.routes(route),
  dir         smallint          NOT NULL,
  start_times interval[]        NOT NULL,
  dates       date[]            NOT NULL
);
CREATE INDEX trips_route_dir_idx
  ON sched.trip_templates (route, dir);

CREATE TABLE sched.segments (
  ttid              text           NOT NULL REFERENCES sched.trip_templates(ttid),
  linkid            integer        NOT NULL REFERENCES nw.links(linkid),
  enter_hms         interval       NOT NULL,
  exit_hms          interval,
  enter_timing      sched.timing_type,
  exit_timing       sched.timing_type,
  enter_rel_dist    double precision,
  exit_rel_dist     double precision,
  PRIMARY KEY (ttid, linkid, enter_hms)
);

COMMIT;
