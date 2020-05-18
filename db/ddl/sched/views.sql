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
    )
  SELECT
   ttid,
   route,
   dir,
   service_date,
   unnest(start_times)      AS start_time,
   (service_date
     || ' Europe/Helsinki')::timestamptz
     + unnest(start_times)  AS start_ts
  FROM unnest_dates
);
COMMENT ON VIEW sched.view_trips IS
'Opens up trip templates into individual trips
with actual start datetimes.';

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
