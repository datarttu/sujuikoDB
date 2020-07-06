DROP TABLE IF EXISTS stage_hfp.jrn_points;
CREATE TABLE stage_hfp.jrn_points (
  jrnid             uuid                  NOT NULL,
  obs_num           bigint                NOT NULL,
  tst               timestamptz           NOT NULL,
  odo               integer,
  drst              boolean,
  geom              geometry(POINT, 3067),
  dodo              double precision,
  dx                double precision,
  spd               double precision,
  acc               double precision,
  hdg               double precision,

  ptid              text,
  seg_candidates    smallint[],
  seg_segno         smallint,         -- ref sched.segments
  seg_linkid        integer,          -- -""-
  seg_reversed      boolean,          -- -""-
  seg_rel_loc       double precision, -- linear loc along seg 0 ... 1
  seg_abs_loc       double precision, -- -""- but absolute 0 ... <seg_length>
  pt_rel_loc        double precision, -- linear loc along pattern geom 0 ... 1
  pt_abs_loc        double precision, -- -""- but absolute 0 ... <seg_length>
  raw_offset        double precision, -- distance raw <-> ref point on seg
  halted_push       double precision, -- how much pushed / pulled (-) along seg when clustering halted points by odo value

  is_redundant      boolean,
  n_rdnt_after      integer,

  PRIMARY KEY (jrnid, obs_num)
);
COMMENT ON TABLE stage_hfp.jrn_points IS
'Points from `stage_hfp.raw` or corresponding temp table
whose `jrnid` represents a valid journey in `stage_hfp.journeys` (or corresp.)
and whose location is within a valid range from the journey shape
(defined when importing points).
Matching points to the trip segments is done using this table.';

CREATE INDEX ON stage_hfp.jrn_points USING GIST(geom);

DROP FUNCTION IF EXISTS stage_hfp.extract_jrn_points_from_raw;
CREATE OR REPLACE FUNCTION stage_hfp.extract_jrn_points_from_raw(
  raw_table       regclass,
  jrn_point_table regclass,
  journey_table   regclass
)
RETURNS bigint
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_ins   bigint;
BEGIN
  EXECUTE format(
    $s$
    WITH inserted AS (
      INSERT INTO %1$s (
        jrnid, obs_num, tst, odo, drst, geom,
        dodo, dx, spd, acc, hdg, ptid
      )
      SELECT
        r.jrnid,
        r.obs_num,
        r.tst,
        r.odo,
        r.drst,
        r.geom,
        r.dodo,
        r.dx,
        r.spd,
        r.acc,
        r.hdg,
        j.ptid
      FROM %2$s                           AS r
      INNER JOIN %3$s                     AS j
        ON r.jrnid = j.jrnid
      RETURNING *
    )
    SELECT count(*) FROM inserted
    $s$,
    jrn_point_table,
    raw_table,
    journey_table
  ) INTO cnt_ins;

  RETURN cnt_ins;
END;
$$;
COMMENT ON FUNCTION stage_hfp.extract_jrn_points_from_raw IS
'Extracts from `raw_table` to `jrn_point_table` points that
have a corresponding `jrnid` journey in `journey_table`.';
