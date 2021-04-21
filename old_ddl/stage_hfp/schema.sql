CREATE SCHEMA IF NOT EXISTS stage_hfp;
COMMENT ON SCHEMA stage_hfp IS
'Staging tables and functions for importing raw HFP data
and transforming and filtering it for `obs` schema.';
