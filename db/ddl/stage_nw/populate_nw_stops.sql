CREATE OR REPLACE FUNCTION stage_nw.populate_nw_stops()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  IF NOT EXISTS (SELECT * FROM nw.nodes LIMIT 1) THEN
    RAISE EXCEPTION 'nw.nodes is empty!'
    USING HINT = 'Run nw.create_node_table first.';
  END IF;

  DELETE FROM nw.stops;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from nw.stops', cnt;

  INSERT INTO nw.stops (
    stopid, nodeid, mode, code, name, descr, parent
  ) (
    SELECT
      a.stopid,
      c.nodeid,
      a.mode,
      a.code,
      a.name,
      a.descr,
      a.parent
    FROM stage_gtfs.stops_with_mode   AS a
    INNER JOIN stage_nw.snapped_stops AS b
      ON a.stopid = b.stopid
    INNER JOIN nw.nodes               AS c
      ON ST_DWithin(b.geom, c.geom, 0.01)
    ORDER BY c.nodeid, a.stopid
  );
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into nw.stops', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.populate_nw_stops IS
'Fill nw.stops stopid-nodeid table
by matching stage_nw.snapped_stops
and additional attributes from GTFS stops
with nw.nodes point locations.
nw.stops will be emptied first.
nw.nodes must be correctly created first.';
