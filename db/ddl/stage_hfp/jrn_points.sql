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
  n_rdnt_after      integer

  PRIMARY KEY (jrnid, obs_num)
);
COMMENT ON TABLE stage_hfp.jrn_points IS
'Points from `stage_hfp.raw` or corresponding temp table
whose `jrnid` represents a valid journey in `stage_hfp.journeys` (or corresp.)
and whose location is within a valid range from the journey shape
(defined when importing points).
Matching points to the trip segments is done using this table.';

CREATE INDEX ON stage_hfp.jrn_points USING GIST(geom_raw);
