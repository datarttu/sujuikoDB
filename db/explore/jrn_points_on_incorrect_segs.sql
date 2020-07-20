BEGIN;

/*
This does NOT fix the situation where the first point
happens to be on a totally wrong segment,
as in the example jrnid!
*/
CREATE FUNCTION strictly_increasing()
RETURNS SETOF stage_hfp.jrn_points
IMMUTABLE STRICT
LANGUAGE PLPGSQL
AS $$
DECLARE
  this_jrnid      uuid;
  this_rec        record;
  last_valid_rec  record;
BEGIN
  FOR this_rec IN (
    SELECT * FROM stage_hfp.jrn_points
    ORDER BY jrnid, obs_num
  ) LOOP
    IF this_rec.jrnid IS DISTINCT FROM this_jrnid THEN
      last_valid_rec  := this_rec;
      this_jrnid      := this_rec.jrnid;
      RETURN NEXT this_rec;
      CONTINUE;
    END IF;
    IF this_rec.pt_abs_loc >= last_valid_rec.pt_abs_loc THEN
      RETURN NEXT this_rec;
    END IF;
  END LOOP;
END;
$$;

CREATE FUNCTION strictly_decreasing()
RETURNS SETOF stage_hfp.jrn_points
IMMUTABLE STRICT
LANGUAGE PLPGSQL
AS $$
DECLARE
  this_jrnid      uuid;
  this_rec        record;
  last_valid_rec  record;
BEGIN
  FOR this_rec IN (
    SELECT * FROM stage_hfp.jrn_points
    ORDER BY jrnid, obs_num DESC
  ) LOOP
    IF this_rec.jrnid IS DISTINCT FROM this_jrnid THEN
      last_valid_rec  := this_rec;
      this_jrnid      := this_rec.jrnid;
      RETURN NEXT this_rec;
      CONTINUE;
    END IF;
    IF this_rec.pt_abs_loc <= last_valid_rec.pt_abs_loc THEN
      RETURN NEXT this_rec;
    END IF;
  END LOOP;
END;
$$;

SELECT count(*) FROM stage_hfp.jrn_points;

SELECT count(*) FROM strictly_increasing();
SELECT count(*) FROM strictly_decreasing();

ROLLBACK;

/*
CREATE TEMP TABLE seg_grp ON COMMIT DROP AS (
WITH
  segment_changes_marked AS (
    SELECT
      jrnid, obs_num, seg_segno,
      CASE WHEN seg_segno IS DISTINCT FROM lag(seg_segno) OVER w
        THEN 1 ELSE 0
      END AS segno_changed
    FROM stage_hfp.jrn_points
    WINDOW w AS (PARTITION BY jrnid ORDER BY obs_num)
  ),
  segment_visit_groups AS (
    SELECT
      jrnid, obs_num, seg_segno,
      sum(segno_changed) OVER w   AS segno_grp
    FROM segment_changes_marked
    WINDOW w AS (PARTITION BY jrnid ORDER BY obs_num)
  )
  SELECT
    jrnid, segno_grp, seg_segno,
    int8range(min(obs_num), max(obs_num), '[]') AS obs_range,
    count(*)                                    AS obs_count
  FROM segment_visit_groups
  GROUP BY jrnid, segno_grp, seg_segno
  ORDER BY jrnid, segno_grp
);

*/

--ROLLBACK;

/*
,
keep_best_segments AS (
  SELECT DISTINCT ON (jrnid, seg_segno)
    jrnid, seg_segno, obs_range, obs_count
  FROM groups_squeezed
  ORDER BY jrnid, seg_segno ASC, obs_count DESC, obs_range ASC
),
to_delete AS (
  SELECT
    jp.jrnid, jp.obs_num, jp.seg_segno
  FROM stage_hfp.jrn_points     AS jp
  LEFT JOIN keep_best_segments  AS bs
    ON  jp.jrnid = bs.jrnid
    AND jp.obs_num <@ bs.obs_range
  WHERE bs.obs_range IS NULL
)
SELECT *
FROM to_delete;
*/
