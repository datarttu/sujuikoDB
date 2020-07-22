DROP TABLE IF EXISTS stage_hfp.jrn_segs;
CREATE TABLE stage_hfp.jrn_segs (
  jrnid             uuid                  NOT NULL,
  segno             smallint              NOT NULL,
  linkid            integer,
  reversed          boolean,
  ij_dist_span      numrange,
  first_used_seg    boolean,
  last_used_seg     boolean,

  -- Arrays
  pt_timestamps     timestamptz[],
  pt_timediffs_s    real[],
  pt_seg_locs_m     real[],
  pt_speeds_m_s     real[],
  pt_doors          boolean[],
  pt_obs_nums       integer[],
  pt_raw_offsets_m  real[],
  pt_halt_offsets_m real[],

  -- First and last values (by timestamp) for interpolation
  fl_timestamps     tstzrange,
  fl_pt_abs_locs    numrange,

  -- Interpolated timestamps
  enter_ts          timestamptz,
  exit_ts           timestamptz,

  -- Aggregates
  thru_s            real,
  halted_s          real,
  door_s            real,
  n_halts           smallint,
  n_valid_obs       smallint,

  PRIMARY KEY (jrnid, segno)
);
COMMENT ON TABLE stage_hfp.jrn_segs IS
'Data from `stage_hfp.jrn_points` or corresponding temp table
collected and aggregated by segment level, with interpolated
enter and exit values at segment ends added (by adjacent segments).
This is the last staging table before inserting to `obs` schema.';

CREATE INDEX ON stage_hfp.jrn_segs USING BTREE(linkid, reversed);
