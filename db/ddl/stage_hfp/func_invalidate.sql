DROP FUNCTION IF EXISTS stage_hfp.invalidate;
CREATE OR REPLACE FUNCTION stage_hfp.invalidate(
  tb_name     text,
  reason      text,
  where_cnd   text
)
RETURNS TABLE (invalid_reason text, rows_affected bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY EXECUTE format(
    'WITH updated AS (
    UPDATE stage_hfp.%1$I
    SET invalid_reasons = array_append(invalid_reasons, %2$L)
    WHERE %3$s
      AND array_position(invalid_reasons, %2$L) IS NULL
    RETURNING *)
    SELECT %2$L, count(*) FROM updated',
    tb_name,
    reason,
    where_cnd
  );
END;
$$;
