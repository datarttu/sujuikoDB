-- TODO: Make into a function.
\set ON_ERROR_STOP on
\set ON_ERROR_ROLLBACK interactive

DELETE FROM stage_hfp.journeys CASCADE;

SELECT stage_hfp.insert_to_journeys_from_raw();

SELECT * FROM stage_hfp.invalidate(
  'journeys',
  'No ongoing points',
  'n_ongoing = 0'
);
SELECT * FROM stage_hfp.invalidate(
  'journeys',
  'NULL odo in > 50 % of points',
  'n_ongoing > 0 AND n_odo_values::real / n_ongoing::real <= 0.5'
);
SELECT * FROM stage_hfp.invalidate(
  'journeys',
  'NULL geom in > 50 % of points',
  'n_ongoing > 0 AND n_odo_values::real / n_ongoing::real <= 0.5'
);
SELECT * FROM stage_hfp.invalidate(
  'journeys',
  'Zero or negative odometer sum',
  'rn_length(odo_span) <= 0'
);
