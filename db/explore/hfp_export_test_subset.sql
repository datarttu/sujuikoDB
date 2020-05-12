COPY (
  SELECT *
  FROM stage_hfp.raw
  WHERE route IN ('1007', '1054', '6173')
)
TO '/data0/hfpdumps/november/test_subset.csv'
WITH CSV HEADER DELIMITER ',';
