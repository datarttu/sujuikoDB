CREATE EXTENSION IF NOT EXISTS btree_gist CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS pgrouting CASCADE;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE FUNCTION append_unique(anyarray, anyelement)
RETURNS anyarray
IMMUTABLE
PARALLEL SAFE
LANGUAGE SQL
AS $$
  SELECT CASE
    WHEN ARRAY[$2] <@ $1 THEN $1
    ELSE array_append($1, $2)
  END;
$$;

-- Thanks for this function and aggregate to Slobodan Pejic at Stackoverflow:
-- https://stackoverflow.com/a/37846454
CREATE FUNCTION coalesce_agg_sfunc(state anyelement, value anyelement)
RETURNS anyelement AS
$function$
  SELECT coalesce(value, state);
$function$ LANGUAGE SQL;

COMMENT ON FUNCTION coalesce_agg_sfunc IS
'Fills a NULL value with the last available non-NULL value.';

CREATE AGGREGATE coalesce_agg(anyelement) (
    SFUNC = coalesce_agg_sfunc,
    STYPE  = anyelement);
COMMENT ON AGGREGATE coalesce_agg(anyelement) IS
'Fills NULL values with the last available non-NULL value according to window ordering.';
