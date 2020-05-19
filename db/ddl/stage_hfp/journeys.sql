CREATE TABLE stage_hfp.journeys (
  jrnid             uuid        PRIMARY KEY,
  start_ts          timestamptz NOT NULL,
  route             text        NOT NULL,
  dir               smallint    NOT NULL CHECK (dir IN (1, 2)),
  oper              smallint    NOT NULL,
  veh               integer     NOT NULL,

  n_total           integer,
  n_ongoing         integer,
  n_dooropen        integer,
  tst_span          tstzrange,

  ttid              text,

  line_raw_length   real,
  line_tt_length    real,
  line_ref_length   real,
  ref_avg_dist      real,
  ref_med_dist      real,
  ref_max_dist      real,
  ref_n_accept      integer,

  invalid_reasons   text[]      DEFAULT '{}'
);

CREATE INDEX ON stage_hfp.journeys (start_ts);
CREATE INDEX ON stage_hfp.journeys (route, dir);
CREATE INDEX ON stage_hfp.journeys (oper, veh);
CREATE INDEX ON stage_hfp.journeys (array_length(invalid_reasons, 1));
