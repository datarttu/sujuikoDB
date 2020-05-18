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
