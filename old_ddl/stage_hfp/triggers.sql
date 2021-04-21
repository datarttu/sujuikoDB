CREATE TRIGGER t10_ignore_invalid_raw_rows
BEFORE INSERT ON stage_hfp.raw
FOR EACH ROW
EXECUTE PROCEDURE stage_hfp.ignore_invalid_raw_rows();

CREATE TRIGGER t20_set_raw_additional_fields
BEFORE INSERT ON stage_hfp.raw
FOR EACH ROW
EXECUTE PROCEDURE stage_hfp.set_raw_additional_fields();
