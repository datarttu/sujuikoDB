-- TODO: Make into a function.
\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK interactive

\ir journeys.sql
\ir journey_points.sql
\ir seg_aggregates.sql

SELECT * FROM stage_hfp.insert_to_journeys_from_raw();

SELECT * FROM invalidate(
  'stage_hfp.journeys',
  'No ongoing points',
  'n_ongoing = 0'
);
SELECT * FROM invalidate(
  'stage_hfp.journeys',
  'NULL odo in > 50 % of points',
  'n_ongoing > 0 AND n_odo_values::real / n_ongoing::real <= 0.5'
);
SELECT * FROM invalidate(
  'stage_hfp.journeys',
  'NULL geom in > 50 % of points',
  'n_ongoing > 0 AND n_odo_values::real / n_ongoing::real <= 0.5'
);
SELECT * FROM invalidate(
  'stage_hfp.journeys',
  'Zero or negative odometer sum',
  'rn_length(odo_span) <= 0'
);

SELECT * FROM stage_hfp.set_journeys_ttid();

SELECT * FROM invalidate(
  'stage_hfp.journeys',
  'No ttid',
  'ttid IS NULL'
);

SELECT * FROM stage_hfp.insert_to_journey_points_from_raw(0.5, 30.0);

SELECT * FROM stage_hfp.insert_to_seg_aggregates();
