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
  i_stop            integer,
  j_stop            integer,
  i_strict          boolean           NOT NULL,
  j_strict          boolean           NOT NULL,
  i_rel_dist        double precision  NOT NULL CHECK (i_rel_dist BETWEEN 0 AND 1),
  j_rel_dist        double precision  NOT NULL CHECK (j_rel_dist BETWEEN 0 AND 1),
  PRIMARY KEY (ttid, i_time, linkid)
);
COMMENT ON TABLE sched.segments IS
'Refers to links that, ordered by enter time `i_time`, comprise the route
geometry of trip template `ttid`.
Values at link enter point are prefixed with `i`, and values at exit point with `j`.
`_time` values are relative to the trip template start, beginning from 0.
`_stop`: stop id if the point is a stop in the template schedule,
         `NULL` if it is a normal node.
`_strict`: true if `_time` at that point is absolute and "planned",
           false if it has been interpolated.
`_rel_dist`: distance relative to the length of the entire trip template geometry.';
