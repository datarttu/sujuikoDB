DROP VIEW IF EXISTS sched.view_trips CASCADE;
CREATE VIEW sched.view_trips AS (
  SELECT
    pt.ptid,
    tp.ttid,
    pt.route,
    pt.dir,
    tt.start_ts,
    (tt.start_ts AT TIME ZONE 'Europe/Helsinki')::date            AS start_date,
    (tt.start_ts AT TIME ZONE 'Europe/Helsinki')::time::interval  AS start_time
  FROM sched.patterns AS pt
  INNER JOIN sched.templates  AS tp
    ON pt.ptid = tp.ptid
  INNER JOIN sched.template_timestamps  AS tt
    ON tp.ttid = tt.ttid
);
COMMENT ON VIEW sched.view_trips IS
'Opens up trip templates into individual trips
with unique trip ids and actual start datetimes.';

DROP VIEW IF EXISTS sched.view_trip_segments CASCADE;
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
    sg.i_stop,
    sg.j_stop,
    tp.start_ts + sg.i_time AS i_ts,
    tp.start_ts + sg.j_time AS j_ts
  FROM sched.view_trips     AS tp
  INNER JOIN sched.segments AS sg
    ON tp.ttid = sg.ttid
);
COMMENT ON VIEW sched.view_trip_segments IS
'Segments of individual trips,
with absolute link enter (i) and exit (j) timestamps.';

DROP VIEW IF EXISTS sched.view_segment_geoms CASCADE;
CREATE VIEW sched.view_segment_geoms AS (
  SELECT
    sg.ttid,
    sg.linkid,
    sg.i_node,
    sg.j_node,
    sg.i_stop,
    sg.j_stop,
    sg.i_time,
    li.cost,
    sum(li.cost) OVER (PARTITION BY sg.ttid ORDER BY sg.i_time) - li.cost AS i_cumul_cost,
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

DROP MATERIALIZED VIEW IF EXISTS sched.mw_trip_template_geoms CASCADE;
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

DROP VIEW IF EXISTS sched.view_trip_template_stops CASCADE;
CREATE VIEW sched.view_trip_template_stops AS (
  WITH stopids AS (
    SELECT ttid, stopid, stoptime
    FROM (
      SELECT ttid, i_stop AS stopid, i_time AS stoptime
      FROM sched.segments
      WHERE i_stop IS NOT NULL
      UNION
      SELECT ttid, j_stop AS stopid, j_time AS stoptime
      FROM sched.segments
      WHERE j_stop IS NOT NULL
    ) AS a
    ORDER BY ttid, stoptime
  )
  SELECT
    si.ttid,
    si.stopid,
    si.stoptime,
    nd.nodeid,
    nd.geom
  FROM stopids AS si
  LEFT JOIN nw.stops  AS st
    ON si.stopid = st.stopid
  LEFT JOIN nw.nodes  AS nd
    ON st.nodeid = nd.nodeid
);
