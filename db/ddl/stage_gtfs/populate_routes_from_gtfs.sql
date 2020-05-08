CREATE FUNCTION stage_gtfs.populate_routes_from_gtfs()
RETURNS TABLE (
 mode            public.mode_type,
 rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
 RETURN QUERY
 WITH
   inserted AS (
     INSERT INTO sched.routes
     SELECT
       route_id AS route,
       (CASE
        WHEN route_type = 0 THEN 'tram'
        WHEN route_type IN (700, 701, 702, 704) THEN 'bus'
        END
       )::public.mode_type AS mode
     FROM stage_gtfs.routes
     ON CONFLICT DO NOTHING
     RETURNING *
   )
 SELECT i.mode, count(i.route)::bigint AS rows_inserted
 FROM inserted AS i
 GROUP BY i.mode
 ORDER BY i.mode;
END;
$$;
COMMENT ON FUNCTION stage_gtfs.populate_routes_from_gtfs IS
'Insert tram and bus routes into sched schema,
with mode indicated as mode_type instead of an integer.
Note that bus mode is NOT based on standard GTFS integer id
but on HSL-specific ids!';
