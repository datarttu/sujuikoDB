DROP VIEW IF EXISTS sched.view_trips;
CREATE VIEW sched.view_trips AS (
  WITH
    unnest_dates AS (
     SELECT
       ttid,
       route,
       dir,
       start_times,
       unnest(dates)  AS service_date
     FROM sched.trip_templates
    ),
    unnest_starttimes AS (
      SELECT
        *,
        unnest(start_times) AS start_time
      FROM unnest_dates
    )
  SELECT
    ttid,
    md5(
      concat_ws(
        '_',
        service_date, start_time, route, dir
      )
    )::uuid         AS tripid,
    route,
    dir,
    service_date,
    start_time,
    (service_date
     || ' Europe/Helsinki')::timestamptz
     + start_time   AS start_ts
  FROM unnest_starttimes
);
COMMENT ON VIEW sched.view_trips IS
'Opens up trip templates into individual trips
with unique trip ids and actual start datetimes.';

DROP VIEW IF EXISTS sched.view_trip_segments;
CREATE VIEW sched.view_trip_segments AS (
  SELECT
    tp.ttid,
    tp.route,
    tp.dir,
    tp.service_date,
    tp.start_time,
    tp.start_ts,
    sg.linkid,
    sg.i_node,
    sg.j_node,
    tp.start_ts + sg.i_time AS i_ts,
    tp.start_ts + sg.j_time AS j_ts
  FROM sched.view_trips     AS tp
  INNER JOIN sched.segments AS sg
    ON tp.ttid = sg.ttid
);
COMMENT ON VIEW sched.view_trip_segments IS
'Segments of individual trips,
with absolute link enter (i) and exit (j) timestamps.';

DROP VIEW IF EXISTS sched.view_segment_geoms;
CREATE VIEW sched.view_segment_geoms AS (
  SELECT
    sg.ttid,
    sg.linkid,
    sg.i_node,
    sg.j_node,
    sg.i_time,
    li.reversed,
    li.geom
  FROM sched.segments                     AS sg
  INNER JOIN nw.view_links_with_reverses  AS li
    ON  sg.linkid = li.linkid
    AND sg.i_node = li.inode
    AND sg.j_node = li.jnode
);
COMMENT ON VIEW sched.view_segment_geoms IS
'Trip template segments with their respective link geometries.
"reversed" indicates if a reversed two-way link geometry is referenced,
by inverting i and j.';

DROP MATERIALIZED VIEW IF EXISTS sched.mw_trip_template_geoms;
CREATE MATERIALIZED VIEW sched.mw_trip_template_geoms AS (
  SELECT
    ttid,
    ST_MakeLine(geom ORDER BY i_time) AS geom
  FROM sched.view_segment_geoms
  GROUP BY ttid
);
COMMENT ON MATERIALIZED VIEW sched.mw_trip_template_geoms IS
'Linestring geometries of entire trip templates.';

CREATE INDEX ON sched.mw_trip_template_geoms USING GIST(geom);
