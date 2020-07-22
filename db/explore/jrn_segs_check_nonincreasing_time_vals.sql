/*
 * Check if there happens to be any invalid segments in jrn_segs,
 * meaning that the timestamp values do not increase as segno increases.
 */
WITH
non_null AS (
  SELECT jrnid, segno, fl_timestamps
  FROM stage_hfp.jrn_segs
  WHERE fl_timestamps IS NOT NULL
),
flags AS (
  SELECT
    jrnid,
    segno,
    fl_timestamps,
    fl_timestamps[1] > lag(fl_timestamps[2]) OVER w AS increases
  FROM non_null
  WINDOW w AS (
    PARTITION BY jrnid
    ORDER BY segno
  )
),
mark_invalid AS (
  SELECT
    *,
    bool_and(increases) OVER (PARTITION BY jrnid) AS all_increase
  FROM flags
)
SELECT *
FROM mark_invalid
WHERE NOT all_increase
ORDER BY jrnid, segno;
