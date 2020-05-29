DROP TABLE IF EXISTS stage_hfp.journey_points CASCADE;
CREATE TABLE stage_hfp.journey_points (
  jrnid             uuid                  NOT NULL REFERENCES stage_hfp.journeys(jrnid),
  tst               timestamptz           NOT NULL,
  event             public.event_type     NOT NULL,
  sub_id            smallint              NOT NULL, -- Some records are duplicated over jrnid, tst, event ...

  odo               integer,
  drst              boolean,
  stop              integer,
  geom              geometry(POINT, 3067),

  -- Running number ordered by 1) tst 2) event
  obs_num           integer               NOT NULL,
  -- Relative rank ordered by 1) tst 2) event
  rel_rank          double precision,

  -- Segment values (calculate by joining the closest corresponding trip template segment)
  seg_linkid        integer,
  seg_reversed      boolean,
  seg_offset        real,
  seg_rel_loc       double precision,
  seg_geom          geometry(POINT, 3067),

  rel_dist          double precision,

  invalid_reasons   text[]                DEFAULT '{}',

  PRIMARY KEY (jrnid, tst, event, sub_id)
);

SELECT *
FROM create_hypertable('stage_hfp.journey_points', 'tst', chunk_time_interval => interval '1 hour');

CREATE INDEX ON stage_hfp.journey_points USING GIST(geom);
CREATE INDEX ON stage_hfp.journey_points USING GIST(seg_geom);
--CREATE INDEX ON stage_hfp.journey_points USING BTREE(jrnid, seg_offset);
--CREATE INDEX ON stage_hfp.journey_points USING BTREE(jrnid, rel_dist);
CREATE INDEX ON stage_hfp.journey_points USING BTREE(cardinality(invalid_reasons));

DROP FUNCTION IF EXISTS stage_hfp.insert_to_journey_points_from_raw;
CREATE OR REPLACE FUNCTION stage_hfp.insert_to_journey_points_from_raw()
RETURNS TABLE (table_name text, rows_inserted bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY
  WITH inserted AS (
    INSERT INTO stage_hfp.journey_points (
      jrnid, tst, event, sub_id, odo, drst, stop, geom, obs_num, rel_rank
    )
      SELECT
        rw.jrnid,
        rw.tst,
        rw.event_type::public.event_type    AS event,
        row_number() OVER (
          PARTITION BY rw.jrnid, rw.tst, rw.event_type
        )                                   AS sub_id,
        rw.odo,
        rw.drst,
        rw.stop,
        rw.geom                             AS geom_orig,
        row_number() OVER w_tst_event       AS obs_num,
        percent_rank() OVER w_tst_event     AS rel_rank
      FROM stage_hfp.raw  AS rw
      INNER JOIN stage_hfp.journeys AS jrn
        ON rw.jrnid = jrn.jrnid
      WHERE rw.is_ongoing IS true
        AND cardinality(jrn.invalid_reasons) = 0
      WINDOW w_tst_event AS (
        PARTITION BY rw.jrnid
        ORDER BY rw.tst, rw.event_type::public.event_type
      )
    RETURNING *
    )
  SELECT 'journey_points', count(*)
  FROM inserted;
END;
$$;
