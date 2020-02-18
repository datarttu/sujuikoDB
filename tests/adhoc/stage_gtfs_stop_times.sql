/*
 * Some queries on stage_gtfs.stop_times.
 * FIXME: Detecting adjacent stop events
 *        with identical dep -> arr times
 *        does not yet work correctly.
 */

\o stage_gtfs_stop_times.out
\qecho Are there adjacent stops with same departure -> arrival times?

CREATE OR REPLACE TEMPORARY VIEW near_identified AS (
  WITH adjacent AS (
    SELECT
      trip_id, arrival_time, departure_time,
      lead(arrival_time) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS next_arrival_time,
      stop_sequence
    FROM stage_gtfs.stop_times
  ),
  identified AS (
    SELECT *
    FROM adjacent
    WHERE departure_time = next_arrival_time
  )
  SELECT
    st.trip_id, st.arrival_time, st.departure_time, st.stop_id,
    s.stop_name, st.stop_sequence,
    (idf.departure_time = idf.next_arrival_time) AS next_same_time
  FROM stage_gtfs.stop_times  AS st
  INNER JOIN stage_gtfs.stops AS s
  ON st.stop_id = s.stop_id
  INNER JOIN identified       AS idf
  ON st.trip_id = idf.trip_id
  AND (st.stop_sequence - idf.stop_sequence) BETWEEN -2 AND 2
);

SELECT *
FROM near_identified
LIMIT 30;

\qecho How many stops with same departure -> arrival times?

SELECT count(trip_id)
FROM near_identified
WHERE next_same_time = TRUE;

\qecho What is the maximum number of adjacent stops with same stop time?

WITH counts AS (
  SELECT trip_id, arrival_time, count(trip_id) AS n_with_same_stoptime
  FROM near_identified
  WHERE next_same_time = TRUE
  GROUP BY trip_id, arrival_time
)
SELECT n_with_same_stoptime, count(n_with_same_stoptime)
FROM counts
GROUP BY n_with_same_stoptime
ORDER BY n_with_same_stoptime DESC;

WITH max_adjacent AS (
  SELECT trip_id, arrival_time, count(trip_id)
  FROM near_identified
  WHERE next_same_time = TRUE
  GROUP BY trip_id, arrival_time
  HAVING count(trip_id) = 20
)
SELECT *
FROM near_identified
WHERE next_same_time = TRUE
AND trip_id IN (SELECT trip_id FROM max_adjacent)
LIMIT 30;

\qecho Are there any stops where departure or arrival time has non-zero seconds (e.g. 14:30:45) ?

CREATE OR REPLACE TEMPORARY VIEW nonzero_secs AS (
  SELECT *
  FROM stage_gtfs.stop_times
  WHERE
    extract(seconds FROM departure_time) <> 0
    OR extract(seconds FROM arrival_time) <> 0
);

SELECT *
FROM nonzero_secs
LIMIT 20;

SELECT count(trip_id)
FROM nonzero_secs;

DROP VIEW nonzero_secs;
DROP VIEW near_identified;

\o
