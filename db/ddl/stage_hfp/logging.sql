DROP TABLE IF EXISTS stage_hfp.discarded_journeys;
CREATE TABLE stage_hfp.discarded_journeys (
  LIKE stage_hfp.journeys
);
ALTER TABLE stage_hfp.discarded_journeys
  ADD COLUMN  added_ts  timestamptz   DEFAULT now(),
  ADD PRIMARY KEY (jrnid, added_ts);
CREATE INDEX ON stage_hfp.discarded_journeys USING BTREE(route, dir);
COMMENT ON TABLE stage_hfp.discarded_journeys IS
'Invalid rows that were discarded from stage_hfp.journeys or corresponding
temporary table, plus metadata fields, for auditing.';
