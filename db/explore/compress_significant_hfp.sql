/*
 * Handling a subset of raw HFP data (one trip),
 * examine how to "compress" the data by leaving out
 * unnecessary rows containing repeated information.
 *
 * 2020-05-11
 */

SELECT
  event_type,
  tst,
  lat,
  lon,
  odo,
  drst,
  stop,
  row_number() OVER (ORDER BY tst)
FROM subset
ORDER BY tst
LIMIT 10;

/*
 * We do not need rows where all of the following field values
 * are practically indifferent from those of the preceding and next row:
 * (- event_type -> DO NOT CONSIDER at this point, we are "blind" to this for now)
 * - tst: always keep if difference > 1 s
 * - lat, lon: always keep if difference > 1 m
 * - drst
 * - stop
 */

CREATE TABLE subset_filtered AS (
WITH
  pts AS (
    SELECT
      event_type,
      tst,
      ST_Transform(
        ST_SetSRID(
          ST_MakePoint(lon, lat),
          4326),
        3067) AS geom,
      odo,
      drst,
      stop
    FROM subset
  ),
  kept AS (
  SELECT
    event_type,
    tst,
    geom,
    odo,
    drst,
    stop,
    row_number() OVER step AS rnum,
    (
      coalesce(tst - lag(tst) OVER step, interval '2 seconds') > interval '1 seconds'
      OR coalesce(lead(tst) OVER step - tst, interval '2 seconds') > interval '1 seconds'
      OR coalesce(ST_Distance(geom, lag(geom) OVER step), 5.0) > 5.0
      OR coalesce(ST_Distance(lead(geom) OVER step, geom), 5.0) > 5.0
      OR coalesce(drst <> lag(drst) OVER step, true)
      OR coalesce(lead(drst) OVER step <> drst, true)
    ) AS keep
  FROM pts
  WINDOW step AS (ORDER BY tst)
  ORDER BY tst
  )
SELECT
  *
FROM kept
WHERE keep
);
ALTER TABLE subset_filtered ADD PRIMARY KEY (rnum);
