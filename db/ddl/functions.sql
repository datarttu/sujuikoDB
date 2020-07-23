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

CREATE OR REPLACE FUNCTION rn_length(int8range)
RETURNS bigint
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT upper($1) - lower($1);
$$;

CREATE OR REPLACE FUNCTION rn_length(numrange)
RETURNS numeric
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT upper($1) - lower($1);
$$;

DROP FUNCTION IF EXISTS minimum_angle;
CREATE FUNCTION minimum_angle(
  x double precision, y double precision
)
RETURNS double precision
LANGUAGE PLPGSQL
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
  ang   double precision;
BEGIN
  IF x IS NULL OR y IS NULL THEN
    RETURN NULL::double precision;
  END IF;
  IF x < y THEN
    ang := abs(y - x);
  ELSE
    ang := abs(x - y);
  END IF;
  IF ang > 180.0 THEN
    RETURN ang - 180.0;
  ELSE
    RETURN ang;
  END IF;
END;
$$;
COMMENT ON FUNCTION minimum_angle IS
'Minimum positive angle [0, 180] between angles `x` and `y`.
`x` and `y` must be between 0 and 360.';

DROP FUNCTION IF EXISTS linear_interpolate;
CREATE FUNCTION linear_interpolate(
  x_i double precision,
  x_0 double precision,
  y_0 timestamptz,
  x_1 double precision,
  y_1 timestamptz
)
RETURNS timestamptz AS
$$
-- See: https://bytefish.de/blog/postgresql_interpolation/
SELECT
  $3 +
  make_interval(secs =>
    (extract(epoch FROM ($5 - $3)) / nullif($4 - $2, 0.0)) *
    ($1 - $2)
  );
$$ LANGUAGE SQL;
COMMENT ON FUNCTION linear_interpolate IS
'Interpolate timestamptz value at location `x_i` that lies between
locations `x_0` and `x_1`, whose timestamps are `y_0` and `y_1` respectively.';

DROP FUNCTION IF EXISTS invalidate;
CREATE OR REPLACE FUNCTION invalidate(
  tb_name     regclass,
  reason      text,
  where_cnd   text
)
RETURNS TABLE (table_name text, invalid_reason text, rows_affected bigint)
VOLATILE
LANGUAGE PLPGSQL
AS $$
BEGIN
  RETURN QUERY EXECUTE format(
    $fmt$
    WITH updated AS (
    UPDATE %1$s
    SET invalid_reasons = array_append(invalid_reasons, %2$L)
    WHERE %3$s
      AND array_position(invalid_reasons, %2$L) IS NULL
    RETURNING *)
    SELECT %1$L, %2$L, count(*) FROM updated
    $fmt$,
    tb_name,
    reason,
    where_cnd
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
