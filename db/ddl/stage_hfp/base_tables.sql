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
