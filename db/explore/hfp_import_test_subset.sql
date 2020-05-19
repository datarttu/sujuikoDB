\timing on

TRUNCATE stage_hfp.raw;

COPY stage_hfp.raw (
  is_ongoing,
  event_type,
  dir,
  oper,
  veh,
  tst,
  lat,
  lon,
  odo,
  drst,
  oday,
  start,
  loc,
  stop,
  route
)
FROM '/data0/hfpdumps/november/hfp_2019-11-02/route_1007.csv'
WITH CSV;

COPY stage_hfp.raw (
  is_ongoing,
  event_type,
  dir,
  oper,
  veh,
  tst,
  lat,
  lon,
  odo,
  drst,
  oday,
  start,
  loc,
  stop,
  route
)
FROM '/data0/hfpdumps/november/hfp_2019-11-02/route_6173.csv'
WITH CSV;

\timing off
