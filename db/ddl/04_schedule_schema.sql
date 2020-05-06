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
  ttid              text              NOT NULL REFERENCES sched.trip_templates(ttid),
  linkid            integer           NOT NULL REFERENCES nw.links(linkid),
  i_node            integer           NOT NULL REFERENCES nw.nodes(nodeid),
  j_node            integer           NOT NULL REFERENCES nw.nodes(nodeid),
  i_time            interval          NOT NULL,
  j_time            interval          NOT NULL,
  i_stop            boolean           NOT NULL,
  j_stop            boolean           NOT NULL,
  i_strict          boolean           NOT NULL,
  j_strict          boolean           NOT NULL,
  i_rel_dist        double precision  NOT NULL CHECK (i_rel_dist BETWEEN 0 AND 1),
  j_rel_dist        double precision  NOT NULL CHECK (j_rel_dist BETWEEN 0 AND 1),
  PRIMARY KEY (ttid, linkid, enter_hms)
);

COMMIT;
