CREATE SCHEMA IF NOT EXISTS sched;
COMMENT ON SCHEMA sched IS
'This schema models public transport operations as they are *planned* to happen on the network.';

CREATE TABLE sched.routes (
  route      text              PRIMARY KEY,
  mode       public.mode_type  NOT NULL
);
COMMENT ON TABLE sched.routes IS
'Public transport services with certain `mode`,
often including inbound and outbound directions,
and possibly variants (e.g. a part of main route ending to a depot)
that are represented by `patterns`.
In addition to route identifiers visible to passengers, `route` has prefix digits
indicating hierarchies within the HSL network: e.g. `1007`, `2550`.';

CREATE TABLE sched.patterns (
  ptid              text              PRIMARY KEY,
  route             text              NOT NULL REFERENCES sched.routes(route),
  dir               smallint          NOT NULL CHECK (dir IN (1, 2)),
  total_dist        real,
  gtfs_shape_id     text
);
CREATE INDEX ON sched.patterns USING BTREE (route, dir);
COMMENT ON TABLE sched.patterns IS
'Variants of `routes` that have a direction `dir = 1 or 2` and a unique itinerary
by which stops are visited on the network.
- `total_dist` is the sum of segment link lengths belonging to the pattern (in meters).
- `gtfs_shape_id` refers to original GTFS data and can be used for comparing
  the network pattern geometry to the GTFS shape geometry.';

CREATE TABLE sched.segments (
  ptid              text              NOT NULL REFERENCES sched.patterns(ptid),
  segno             smallint          NOT NULL CHECK (segno > 0),
  linkid            integer           NOT NULL REFERENCES nw.links(linkid),
  reversed          boolean           NOT NULL,
  ij_stops          integer[]             NULL CHECK (cardinality(ij_stops) = 2),
  ij_dist_span      numrange          NOT NULL,
  stop_seq          smallint          NOT NULL,

  PRIMARY KEY (ptid, segno)
);
CREATE INDEX ON sched.segments USING BTREE (linkid, reversed);
CREATE INDEX ON sched.segments USING GIN (ij_stops)
  WHERE ij_stops[1] IS NOT NULL OR ij_stops[2] IS NOT NULL;
CREATE INDEX ON sched.segments USING GIST (ij_dist_span);
CREATE INDEX ON sched.segments USING BTREE (ptid, stop_seq);
COMMENT ON TABLE sched.segments IS
'Itineraries of patterns `ptid` represented by links `linkid` ordered by `segno`.
- If `reversed IS true` then that link is traversed opposite to the original linestring direction
  (must be two-way link of course).
- Non-null `ij_stops[1]` indicates a stop id belonging to the pattern at the start of the segment,
  `ij_stops[2]` a stop id at the end of the segment.
- `ij_dist_span` indicates the total distance of the pattern traveled at the start and end of the link:
  these ranges must not overlap among segments belonging to the same `ptid`.
- `stop_seq` groups the segments within `ptid` by the order in which stops are visited:
  each `ij_stops[1]` value starts a new sequence number.';

CREATE TABLE sched.templates (
  ttid              text              PRIMARY KEY,
  ptid              text              NOT NULL REFERENCES sched.patterns(ptid),
  start_times       interval[]        NOT NULL,
  dates             date[]            NOT NULL,
  gtfs_trip_ids     text[]
);
CREATE INDEX ON sched.templates USING BTREE (ptid);
CREATE INDEX ON sched.templates USING GIN (start_times);
CREATE INDEX ON sched.templates USING GIN (dates);
CREATE INDEX ON sched.templates USING GIN (gtfs_trip_ids);
COMMENT ON TABLE sched.templates IS
'Realizations of patterns `ptid` that share common operating times (see `segment_times`),
initial departure times `start_times` and operating days `dates`.
- `gtfs_trip_ids` contains the GTFS trip ids of which the template has been originally composed.';

CREATE TABLE sched.segment_times (
  ttid              text              NOT NULL REFERENCES sched.templates(ttid),
  segno             smallint          NOT NULL,
  ij_times          interval[]        NOT NULL CHECK (cardinality(ij_times) = 2),
  ij_timepoints     boolean[]         NOT NULL CHECK (cardinality(ij_timepoints) = 2),

  PRIMARY KEY (ttid, segno)
);
CREATE INDEX ON sched.segment_times USING GIN (ij_timepoints)
  WHERE ij_timepoints[1] IS NOT NULL OR ij_timepoints[2] IS NOT NULL;
COMMENT ON TABLE sched.segment_times IS
'Operation schedules of templates `ttid` on the segments of respective pattern `ptid`:
think of as an "extension" of `segments`, since one pattern can have several different operating times.
- `segno` refers to the segment number of the respective `ptid` in `segments`.
- `ij_times[1]` indicates time difference to the initial departure time at the start of the segment,
  `ij_times[2]` at the end of the segment.
- `ij_timepoints[1]` indicates if the start of the segment is used as time equalization stop in the schedule,
  `ij_timepoints[2]` indicates it for the end of the segment.';
