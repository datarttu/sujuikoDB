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
FROM '/data0/hfpdumps/november/test_subset.csv'
WITH CSV HEADER;
