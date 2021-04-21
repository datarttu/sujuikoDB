CREATE OR REPLACE FUNCTION stage_gtfs.fix_direction_id()
RETURNS TABLE (
  table_name    text,
  rows_affected bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cur_vals  smallint[];
  cnt       bigint;
BEGIN
  SELECT INTO cur_vals
    array_agg(direction_id ORDER BY direction_id)
  FROM (
    SELECT DISTINCT direction_id
    FROM stage_gtfs.trips
  ) AS a;
  IF cur_vals = ARRAY[1, 2]::smallint[] THEN
    RAISE NOTICE 'direction_id values already set to 1, 2';
    RETURN QUERY SELECT 'stage_gtfs.trips' AS table_name, 0::bigint AS rows_affected;
  ELSIF cur_vals <> ARRAY[0, 1]::smallint[] THEN
    RAISE EXCEPTION 'Invalid direction_id values in stage_gtfs.trips: %', cur_vals::text;
  ELSE
    RAISE NOTICE 'Updating direction_id values 0, 1 to 1, 2 in stage_gtfs.trips ...';
    RETURN QUERY
    WITH updated AS (
      UPDATE stage_gtfs.trips
      SET direction_id = direction_id + 1
      RETURNING *
    )
    SELECT 'stage_gtfs.trips' AS table_name, count(*) AS rows_affected
    FROM updated;
  END IF;
END;
$$;
