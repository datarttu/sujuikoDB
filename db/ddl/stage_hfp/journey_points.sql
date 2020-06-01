DROP TABLE IF EXISTS stage_hfp.journey_points CASCADE;
CREATE TABLE stage_hfp.journey_points (
  jrnid             uuid                  NOT NULL REFERENCES stage_hfp.journeys(jrnid),
  tst               timestamptz           NOT NULL,
  sub_id            smallint              NOT NULL, -- Some records are duplicated over jrnid and tst ...

  odo               integer,
  drst              boolean,
  stop              integer,
  geom              geometry(POINT, 3067),

  -- Running number, calculated before insignificant observations were omitted
  obs_num           integer               NOT NULL,
  -- Relative rank ordered by tst
  rel_rank          double precision,

  -- Segment values (calculate by joining the closest corresponding trip template segment)
  seg_linkid        integer,
  seg_reversed      boolean,
  seg_offset        real,
  seg_rel_loc       double precision,
  seg_pt_geom       geometry(POINT, 3067),

  rel_dist          double precision,

  invalid_reasons   text[]                DEFAULT '{}',

  PRIMARY KEY (jrnid, tst, sub_id)
);

SELECT *
FROM create_hypertable('stage_hfp.journey_points', 'tst', chunk_time_interval => interval '1 hour');

CREATE INDEX ON stage_hfp.journey_points USING GIST(geom);
CREATE INDEX ON stage_hfp.journey_points USING GIST(seg_pt_geom);
--CREATE INDEX ON stage_hfp.journey_points USING BTREE(jrnid, seg_offset);
--CREATE INDEX ON stage_hfp.journey_points USING BTREE(jrnid, rel_dist);
CREATE INDEX ON stage_hfp.journey_points USING BTREE(cardinality(invalid_reasons));

DROP FUNCTION IF EXISTS stage_hfp.insert_to_journey_points_from_raw;
CREATE OR REPLACE FUNCTION stage_hfp.insert_to_journey_points_from_raw(
  max_gps_tolerance  real
)
RETURNS TABLE (table_name text, rows_inserted bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY
  WITH
    ongoing_vp_valid AS (
      SELECT r.jrnid, r.tst, r.odo, r.drst, r.stop, r.geom
      FROM stage_hfp.raw            AS r
      INNER JOIN stage_hfp.journeys AS j
        ON r.jrnid = j.jrnid
      WHERE r.is_ongoing IS true
        AND r.event_type = 'VP'
        AND cardinality(j.invalid_reasons) = 0
    ),
    mark_significant_observations AS (
      SELECT
        jrnid, tst, odo, drst, stop, geom,
        row_number() OVER (PARTITION BY jrnid, tst) AS obs_num,
        CASE WHEN (
              odo   IS DISTINCT FROM lag(odo)   OVER w_tst
          OR  odo   IS DISTINCT FROM lead(odo)  OVER w_tst
          OR  drst  IS DISTINCT FROM lag(drst)  OVER w_tst
          OR  drst  IS DISTINCT FROM lead(drst) OVER w_tst
          OR  ST_Distance(geom, lag(geom) OVER w_tst) > max_gps_tolerance
          OR  ST_Distance(geom, lead(geom) OVER w_tst) > max_gps_tolerance
          ) THEN true
          ELSE false
        END AS is_significant
      FROM ongoing_vp_valid
      WINDOW w_tst AS (PARTITION BY jrnid ORDER BY tst)
    ),
    inserted AS (
      INSERT INTO stage_hfp.journey_points (
        jrnid, tst, sub_id, odo, drst, stop, geom, obs_num, rel_rank
      )
        SELECT
          jrnid,
          tst,
          row_number() OVER (PARTITION BY jrnid, tst) AS sub_id,
          odo,
          drst,
          stop,
          geom,
          obs_num,
          percent_rank() OVER (PARTITION BY jrnid ORDER BY tst)
        FROM mark_significant_observations
      RETURNING *
    )
  SELECT 'journey_points', count(*)
  FROM inserted;
END;
$$;

DROP FUNCTION IF EXISTS stage_hfp.set_journey_points_segment_vals;
CREATE OR REPLACE FUNCTION stage_hfp.set_journey_points_segment_vals()
RETURNS TABLE (table_name text, rows_affected bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY

  WITH
    segs_with_relranks AS (
      SELECT
        sg.ttid,
        sg.linkid,
        sg.i_node,
        sg.j_node,
        sg.i_rel_dist,
        sg.j_rel_dist,
        percent_rank() OVER (
          PARTITION BY sg.ttid
          ORDER BY sg.i_time
        )                       AS seg_rel_rank,
        li.geom                 AS seg_geom,
        li.reversed
      FROM sched.segments                     AS sg
      INNER JOIN nw.view_links_with_reverses  AS li
        ON sg.linkid = li.linkid
        AND sg.i_node = li.inode
        AND sg.j_node = li.jnode
      WHERE sg.ttid IN (
        SELECT DISTINCT ttid
        FROM stage_hfp.journeys
      )
    ),
    to_update AS (
      SELECT
        jp.jrnid,
        jp.tst,
        jp.event,
        jp.sub_id,
        sg.linkid,
        sg.reversed,
        sg.i_rel_dist,
        sg.j_rel_dist,
        sg.seg_geom
      FROM stage_hfp.journey_points AS jp
      INNER JOIN stage_hfp.journeys AS jrn
        ON jp.jrnid = jrn.jrnid
      INNER JOIN LATERAL (
          SELECT swr.*
          FROM segs_with_relranks AS swr
          WHERE swr.ttid = jrn.ttid
          ORDER BY
            jp.geom <-> swr.seg_geom,
            abs(jp.rel_rank - swr.seg_rel_rank)
          LIMIT 1
        ) AS sg
        ON true
    ),
    updated AS (
      UPDATE stage_hfp.journey_points AS upd
      SET
        seg_linkid    = tu.linkid,
        seg_reversed  = tu.reversed,
        seg_offset    = ST_Distance(geom, tu.seg_geom),
        seg_rel_loc   = ST_LineLocatePoint(tu.seg_geom, geom),
        seg_pt_geom   = ST_ClosestPoint(tu.seg_geom, geom),
        rel_dist      = tu.i_rel_dist + ST_LineLocatePoint(tu.seg_geom, geom) * (tu.j_rel_dist - tu.i_rel_dist)
      FROM (
        SELECT * FROM to_update
      ) AS tu
      WHERE upd.jrnid   = tu.jrnid
        AND upd.tst     = tu.tst
        AND upd.event   = tu.event
        AND upd.sub_id  = tu.sub_id
      RETURNING *
    )
  SELECT 'journey_points', count(*)
  FROM updated;
END;
$$;
