CREATE TABLE stage_gtfs.shape_lines (
 shape_id              text        PRIMARY KEY,
 gtfs_dist_total       double precision,
 geom                  geometry(LINESTRING, 3067),
 wgs_geom              geometry(LINESTRING, 4326)
);
CREATE INDEX ON stage_gtfs.shape_lines
USING GIST(geom);
CREATE INDEX ON stage_gtfs.shape_lines
USING GIST(wgs_geom);
COMMENT ON TABLE stage_gtfs.shape_lines IS
'GTFS trip geometries as in original "shapes" table,
but each record is a linestring instead of a point.
Includes both TM35 and WGS84 geometries.';

CREATE FUNCTION stage_gtfs.populate_shape_lines()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
 cnt   integer;
BEGIN

 DELETE FROM stage_gtfs.shape_lines;
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE '% rows deleted from stage_gtfs.shape_lines', cnt;

 WITH
   shape_pts AS (
     SELECT
       shape_id,
       shape_pt_sequence,
       shape_dist_traveled,
       ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326) AS pt_geom
     FROM stage_gtfs.shapes
   )
 INSERT INTO stage_gtfs.shape_lines (
   shape_id, gtfs_dist_total, wgs_geom
 )
 SELECT
   shape_id,
   max(shape_dist_traveled) AS gtfs_dist_total,
   ST_MakeLine(pt_geom ORDER BY shape_pt_sequence) AS geom
 FROM shape_pts
 GROUP BY shape_id;
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE '% rows inserted into stage_gtfs.shape_lines', cnt;

 UPDATE stage_gtfs.shape_lines
 SET geom = ST_Transform(wgs_geom, 3067);
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE 'geom set for % rows in stage_gtfs.shape_lines', cnt;

 RETURN 'OK';
END;
$$;
