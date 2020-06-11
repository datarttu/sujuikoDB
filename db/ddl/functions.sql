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

DROP FUNCTION IF EXISTS propagate_invalidations(text, text, text);
CREATE OR REPLACE FUNCTION propagate_invalidations(
  propagate_from  text,
  propagate_to    text,
  key             text
)
RETURNS TABLE (
  table_name      text,
  invalid_reason  text,
  rows_affected   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  msg         text;
  from_arr    text[];
  to_arr      text[];
  from_schema text;
  from_table  text;
  to_schema   text;
  to_table    text;
BEGIN
  msg := format('Invalid records in %s', propagate_from);

  from_arr := string_to_array(propagate_from, '.');
  IF cardinality(from_arr) = 1 THEN
    from_schema := 'public';
    from_table := from_arr[1];
  ELSIF cardinality(from_arr) > 2 THEN
    RAISE EXCEPTION 'Too many "." in propagate_from: should be "schema.table" or "table"';
  ELSE
    from_schema := from_arr[1];
    from_table := from_arr[2];
  END IF;

  to_arr := string_to_array(propagate_to, '.');
  IF cardinality(to_arr) = 1 THEN
    to_schema := 'public';
    to_table := to_arr[1];
  ELSIF cardinality(to_arr) > 2 THEN
    RAISE EXCEPTION 'Too many "." in propagate_to: should be "schema.table" or "table"';
  ELSE
    to_schema := to_arr[1];
    to_table := to_arr[2];
  END IF;

  RETURN QUERY EXECUTE format(
    $fmt$
    WITH updated AS (
      UPDATE %1$I.%2$I AS upd
      SET invalid_reasons = array_append(invalid_reasons, %5$L)
      FROM (
        SELECT DISTINCT %6$I
        FROM %3$I.%4$I
        WHERE invalid_reasons IS NOT NULL
          AND cardinality(invalid_reasons) > 0
      ) AS ivd
      WHERE ivd.%6$I = upd.%6$I
        AND array_position(upd.invalid_reasons, %5$L) IS NULL
      RETURNING *)
    SELECT %7$L, %5$L, count(*) FROM updated
    $fmt$,
    to_schema,
    to_table,
    from_schema,
    from_table,
    msg,
    key,
    propagate_to
  );
END;
$$;
