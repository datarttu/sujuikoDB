DROP TABLE IF EXISTS stage_hfp.seg_aggregates;
CREATE TABLE stage_hfp.seg_aggregates (
  jrnid           uuid            NOT NULL,
  linkid          integer         NOT NULL,
  reversed        boolean         NOT NULL,

  n_obs           integer,
  i_time          timestamptz     NOT NULL,
  ij_secs         real,

  i_stopped_secs  real,
  j_stopped_secs  real,
  i_door_secs     real,
  j_door_secs     real,

  PRIMARY KEY (jrnid, i_time)
);

CREATE INDEX ON stage_hfp.seg_aggregates USING BTREE(linkid, reversed);

DROP FUNCTION IF EXISTS stage_hfp.insert_to_seg_aggregates;
CREATE OR REPLACE FUNCTION stage_hfp.insert_to_seg_aggregates()
RETURNS TABLE (table_name text, rows_inserted bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY
  WITH
    inserted AS (
      INSERT INTO stage_hfp.seg_aggregates (
        jrnid, linkid, reversed,
        n_obs, i_time, ij_secs,
        i_stopped_secs, j_stopped_secs, i_door_secs, j_door_secs
      )
      SELECT
        jrnid,
        seg_linkid    AS linkid,
        seg_reversed  AS reversed,
        count(*)  AS n_obs,
        min(tst)  AS i_time,
        extract(epoch FROM max(tst) - min(tst)) AS ij_secs,
        coalesce(sum(dt_ahead) filter(WHERE d_odo_ahead = 0 AND seg_rel_loc < 0.5), 0) AS i_stopped_secs,
        coalesce(sum(dt_ahead) filter(WHERE d_odo_ahead = 0 AND seg_rel_loc >= 0.5), 0) AS j_stopped_secs,
        coalesce(sum(dt_ahead) filter(WHERE drst IS true AND seg_rel_loc < 0.5), 0) AS i_door_secs,
        coalesce(sum(dt_ahead) filter(WHERE drst IS true AND seg_rel_loc >= 0.5), 0) AS j_door_secs
      FROM stage_hfp.journey_points
      WHERE seg_linkid IS NOT NULL
        AND seg_reversed IS NOT NULL
      GROUP BY jrnid, linkid, reversed
      ORDER BY jrnid, i_time

      RETURNING *
    )
    SELECT 'seg_aggregates', count(*)
    FROM inserted;
END;
$$;

DROP VIEW IF EXISTS stage_hfp.test_summarise_on_segs;
CREATE VIEW stage_hfp.test_summarise_on_segs AS (
  WITH speeds AS (
    SELECT
      sa.linkid,
      sa.reversed,
      CASE
        WHEN sa.ij_secs = 0 THEN 0
        ELSE li.cost / sa.ij_secs * 3.6
      END AS spd_kmh,
      i_stopped_secs + j_stopped_secs AS secs_stopped
    FROM stage_hfp.seg_aggregates AS sa
    INNER JOIN nw.links           AS li
      ON sa.linkid = li.linkid
    WHERE sa.n_obs > 3
  )
  SELECT
    linkid,
    reversed,
    avg(spd_kmh)          AS avg_kmh,
    stddev_samp(spd_kmh)  AS std,
    avg(secs_stopped) filter(WHERE secs_stopped > 0)  AS avg_stopped,
    count(*)              AS n_journeys
  FROM speeds
  GROUP BY linkid, reversed
);


WITH
  seg_subset AS (
    SELECT linkid, reversed, i_cumul_cost, i_stop
    FROM sched.view_segment_geoms
    WHERE ttid = '1088_1_9'
      AND i_time < '00:04:21'::interval
  )
SELECT
  p.jrnid,
  j.route,
  j.dir,
  j.start_ts,
  p.tst,
  p.odo,
  p.drst,
  p.seg_linkid,
  p.seg_reversed,
  p.abs_dist,
  p.dx_ahead,
  p.dt_ahead
FROM stage_hfp.journey_points AS p
INNER JOIN seg_subset         AS s
  ON p.seg_linkid = s.linkid
  AND p.seg_reversed = s.reversed
INNER JOIN stage_hfp.journeys AS j
  ON p.jrnid = j.jrnid
