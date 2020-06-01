\timing on

BEGIN;

DELETE FROM stage_hfp.raw
WHERE event_type <> 'VP'
  OR NOT is_ongoing;

WITH
  windowed AS (
    SELECT
      jrnid, tst, odo, drst, stop, geom,
      row_number() OVER (PARTITION BY jrnid, tst) AS obs,
      CASE WHEN (
        odo IS DISTINCT FROM lag(odo) OVER w_tst
        OR odo IS DISTINCT FROM lead(odo) OVER w_tst
        OR drst IS DISTINCT FROM lag(drst) OVER w_tst
        OR drst IS DISTINCT FROM lead(drst) OVER w_tst
        OR ST_Distance(geom, lag(geom) OVER w_tst) > 0.5
        OR ST_Distance(geom, lead(geom) OVER w_tst) > 0.5
      ) THEN true
      ELSE false
      END AS is_significant,
      round(ST_Distance(geom, lag(geom) OVER w_tst)::numeric, 2)
      || '; '
      || round(ST_Distance(geom, lead(geom) OVER w_tst)::numeric, 2) AS distances
    FROM stage_hfp.raw
    WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    ORDER BY jrnid, tst
  )
SELECT * FROM windowed WHERE obs > 1;
-- DELETE FROM stage_hfp.raw AS r
-- USING windowed AS w
-- WHERE r.jrnid = w.jrnid
--   AND r.tst = w.tst
--   AND ;

\timing off
