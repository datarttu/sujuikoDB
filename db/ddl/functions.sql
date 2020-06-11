CREATE OR REPLACE FUNCTION rn_length(tstzrange)
RETURNS interval
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS
$$
  SELECT upper($1) - lower($1);
$$;

CREATE OR REPLACE FUNCTION rn_length(int4range)
RETURNS integer
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT upper($1) - lower($1);
$$;

DROP FUNCTION IF EXISTS invalidate;
CREATE OR REPLACE FUNCTION invalidate(
  tb_name     text,
  reason      text,
  where_cnd   text
)
RETURNS TABLE (table_name text, invalid_reason text, rows_affected bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
DECLARE
  tbname_arr  text[];
  schemaname  text;
  tablename   text;
BEGIN
  tbname_arr := string_to_array(tb_name, '.');
  IF cardinality(tbname_arr) = 1 THEN
    schemaname := 'public';
    tablename := tbname_arr[1];
  ELSIF cardinality(tbname_arr) > 2 THEN
    RAISE EXCEPTION 'Too many "." in table_name: should be "schema.table" or "table"';
  ELSE
    schemaname := tbname_arr[1];
    tablename := tbname_arr[2];
  END IF;

  RETURN QUERY EXECUTE format(
    $fmt$
    WITH updated AS (
    UPDATE %1$I.%2$I
    SET invalid_reasons = array_append(invalid_reasons, %3$L)
    WHERE %4$s
      AND array_position(invalid_reasons, %3$L) IS NULL
    RETURNING *)
    SELECT %5$L, %3$L, count(*) FROM updated
    $fmt$,
    schemaname,
    tablename,
    reason,
    where_cnd,
    schemaname || '.' || tablename
  );
END;
$$;
