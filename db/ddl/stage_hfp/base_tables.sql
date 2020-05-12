CREATE TABLE stage_hfp.raw (
  is_ongoing    boolean,
  event_type    text,
  dir           smallint,
  oper          smallint,
  veh           integer,
  tst           timestamptz   NOT NULL,
  lat           real,
  lon           real,
  odo           integer,
  drst          boolean,
  oday          date,
  start         interval,
  loc           text,
  stop          integer,
  route         text
);

CREATE INDEX ON stage_hfp.raw USING BTREE (route, dir);
CREATE INDEX ON stage_hfp.raw USING BRIN (oday, start);
CREATE INDEX ON stage_hfp.raw USING BTREE (is_ongoing);
SELECT *
FROM create_hypertable('stage_hfp.raw', 'tst', chunk_time_interval => interval '1 hour');
