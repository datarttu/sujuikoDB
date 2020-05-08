CREATE VIEW sched.individual_trips AS (
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
   unnest(start_times) AS start_time
  FROM unnest_dates
);
COMMENT ON VIEW sched.individual_trips IS
'Opens up trip templates into individual trips
with start times and service dates.';
