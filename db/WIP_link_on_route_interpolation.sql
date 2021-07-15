BEGIN;

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

CREATE FUNCTION obs.get_interpolated_enter_timestamps()
RETURNS TABLE (
  jrnid     uuid,
  link_seq  integer,
  x0        float8,
  t0        timestamptz,
  x1        float8,
  t1        timestamptz,
  x         float8,
  t         timestamptz
)
STABLE
LANGUAGE SQL
AS $function$
WITH
  -- We want to get the first and last space-time data points from each link on the
  -- journey route and use them as source data for interpolating the time values at link
  -- boundaries, i.e. where relative location values are 0 (start) and 1 (end).
  -- first() and last() are TimescaleDB functions that allow us to select
  -- the "edge" location values from the rows with min and max timestamp directly.
  edge_points AS (
    SELECT
      jrnid,
      link_seq,
      min(tst)                      AS first_tst,
      first(location_on_link, tst)  AS first_rel_loc,
      max(tst)                      AS last_tst,
      last(location_on_link, tst)   AS last_rel_loc
    FROM obs.point_on_link
    GROUP BY jrnid, link_seq
  ),
  -- Now we have the source data for interpolation,
  -- but we are probably missing values from some links_on_route,
  -- especially from very short links.
  -- Also there will be links with just one observation, but that should not be
  -- a problem: the same value just works as x1/t1 point for previous interpolation
  -- and x0/t0 point for the next one.
  -- Since we want to interpolate also the links without observations
  -- (as long as they have observations on other links before or after them),
  -- we use nw.link_on_route to get the full set of ordered links belonging to
  -- the journey as it was planned. nw.view_link_directed is used simply to get
  -- the link lengths so we can convert relative locations to absolute ones.
  -- Cumulative sum over a window is used already here with the link length
  -- to calculate link start distance values from the beginning of the route,
  -- which are then used as a basis for data point distance values.
  complete_route_links AS (
    SELECT
      jrn.jrnid,
      lor.link_seq,
      vld.length_m,
      sum(vld.length_m) OVER w_link - vld.length_m  AS cumul_length_m,
      ep.first_tst,
      ep.first_rel_loc * vld.length_m AS first_loc_m,
      ep.last_tst,
      ep.last_rel_loc * vld.length_m  AS last_loc_m
    FROM nw.link_on_route             AS lor
    INNER JOIN nw.view_link_directed  AS vld
      ON (lor.link_id = vld.link_id AND lor.link_reversed = vld.link_reversed)
    INNER JOIN obs.journey            AS jrn
      ON (lor.route_ver_id = jrn.route_ver_id)
    LEFT JOIN edge_points             AS ep
      ON (jrn.jrnid = ep.jrnid AND lor.link_seq = ep.link_seq)
    WINDOW w_link AS (PARTITION BY jrn.jrnid ORDER BY lor.link_seq)
  ),
  -- Next we fill the missing x/t observation values for empty links:
  -- for x0/t0 pairs we use the last available value (window starting from link_seq=1)
  -- and for x1/t1 pairs the next available value (window starting from the end of link_seq).
  -- It is important to note that only the observation values are carried forwards/backwards
  -- and distance value at link start (x) is kept as it is: this way we will
  -- get the correct timestamp (t) for each link start.
  nulls_filled AS (
    SELECT
      jrnid,
      link_seq,
      cumul_length_m,
      coalesce_agg(last_tst) OVER w_link_forth                    AS last_avail_tst,
      coalesce_agg(cumul_length_m + last_loc_m) OVER w_link_forth AS last_avail_loc_m,
      coalesce_agg(first_tst) OVER w_link_back                    AS first_avail_tst,
      coalesce_agg(cumul_length_m + first_loc_m) OVER w_link_back AS first_avail_loc_m
    FROM complete_route_links
    WINDOW
      w_link_forth  AS (PARTITION BY jrnid ORDER BY link_seq),
      w_link_back   AS (PARTITION BY jrnid ORDER BY link_seq DESC)
  ),
  -- Interpolated timestamp (t) value is calculated for the start of each link.
  -- x1/t1 values are therefore obtained from link N while
  -- x0/t0 values are obtained from link N-1.
  -- Technically this means that the first link gets no interpolated t at start,
  -- which makes sense in practice too, because we don't have any observations
  -- before the first link to interpolate with.
  -- At this point, we assign the interpolation parameters shorter names
  -- so the formula will be more readable in the end.
  interpolation_parameters AS (
    SELECT
      jrnid,
      link_seq,
      lag(last_avail_tst) OVER w_link   AS t0,
      lag(last_avail_loc_m) OVER w_link AS x0,
      first_avail_tst                   AS t1,
      first_avail_loc_m                 AS x1,
      cumul_length_m                    AS x
    FROM nulls_filled
    WINDOW w_link AS (PARTITION BY jrnid ORDER BY link_seq)
  )
SELECT
  jrnid,
  link_seq,
  x0,
  t0,
  x1,
  t1,
  x,
  -- To enable basic calculations and relating with distance values,
  -- timestamp values must be converted to N of seconds and intervals.
  -- timestamptz + interval will automatically result in timestamptz.
  t0 + ( (x - x0) * (extract(epoch FROM t1 - t0) / (x1 - x0)) * interval '1 second') AS t
FROM interpolation_parameters;
$function$;

COMMENT ON FUNCTION obs.get_interpolated_enter_timestamps() IS
'From obs.point_on_link and nw.link_on_route, interpolates enter timestamps (t) at link start locations (x) using last available points before the link (x0, t0) and first available point on or after the link (x1, t1).';

SELECT *
FROM obs.get_interpolated_enter_timestamps()
WHERE jrnid = 'cd0cbca5-faf6-80d8-909e-06b720552f9b'
  AND link_seq > 170
ORDER BY link_seq;

ROLLBACK;
