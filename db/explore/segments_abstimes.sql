BEGIN;

CREATE VIEW sched.individual_trip_segments AS (
  WITH
    trip_departures AS (
      SELECT
        ttid,
        route,
        dir,
        service_date,
        start_time,
        (service_date
          || ' Europe/Helsinki')::timestamptz
          + start_time         AS dep_ts
      FROM sched.individual_trips
    )
  SELECT
    td.*,
    sg.linkid,
    sg.i_node,
    sg.j_node,
    td.dep_ts + sg.i_time AS i_ts,
    td.dep_ts + sg.j_time AS j_ts
  FROM trip_departures      AS td
  INNER JOIN sched.segments AS sg
    ON td.ttid = sg.ttid
);

SELECT
  time_bucket('1 hour', i_ts) AS tb,
  count(*)
FROM sched.individual_trip_segments
WHERE linkid = 1002
  AND i_ts BETWEEN '2019-11-12 06:00:00+02' AND '2019-11-13 06:00:00+02'
GROUP BY tb
ORDER BY tb;

ROLLBACK;
