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

DROP MATERIALIZED VIEW IF EXISTS sched.mw_pattern_shapes CASCADE;
CREATE MATERIALIZED VIEW sched.mw_pattern_shapes AS (
  SELECT
    pt.ptid,
    pt.route,
    pt.dir,
    pt.total_dist,
    pt.gtfs_shape_id,
    ST_MakeLine(lwr.geom ORDER BY sg.segno) AS geom
  FROM sched.patterns       AS pt
  INNER JOIN sched.segments AS sg
    ON pt.ptid = sg.ptid
  INNER JOIN nw.mw_links_with_reverses  AS lwr
    ON  sg.linkid = lwr.linkid
    AND sg.reversed = lwr.reversed
  GROUP BY pt.ptid
);
COMMENT ON MATERIALIZED VIEW sched.mw_pattern_shapes IS
'Linestring geometries and common attributes of entire patterns.';
CREATE INDEX ON sched.mw_pattern_shapes USING GIST(geom);

DROP MATERIALIZED VIEW IF EXISTS sched.mw_pattern_stops CASCADE;
CREATE MATERIALIZED VIEW sched.mw_pattern_stops AS (
  WITH
    stopids AS (
      SELECT
        ptid,
        unnest(ij_stops)  AS stopid,
        segno
      FROM sched.segments
    ),
    unique_stopids AS (
      SELECT
        ptid,
        stopid,
        max(segno) AS segno
      FROM stopids
      WHERE stopid IS NOT NULL
      GROUP BY ptid, stopid
      ORDER BY ptid, segno
    )
  SELECT
    us.ptid,
    pt.route,
    pt.dir,
    us.stopid,
    -- This is because otherwise last stop would not get the correct stop seq:
    row_number() OVER (PARTITION BY us.ptid ORDER BY us.segno)  AS stop_seq,
    st.code,
    st.name,
    st.parent,
    nd.nodeid,
    nd.geom
  FROM unique_stopids       AS us
  INNER JOIN sched.patterns AS pt
    ON us.ptid = pt.ptid
  INNER JOIN nw.stops       AS st
    ON us.stopid = st.stopid
  INNER JOIN nw.nodes       AS nd
    ON st.nodeid = nd.nodeid
);
COMMENT ON MATERIALIZED VIEW sched.mw_pattern_stops IS
'Stop point geometries belonging to patterns
with common pattern and stop attributes.';
CREATE INDEX ON sched.mw_pattern_stops USING GIST (geom);
