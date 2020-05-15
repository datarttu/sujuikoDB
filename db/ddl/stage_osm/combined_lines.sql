/*
 * Following tables are created by ogr2ogr when importing data:
 * CREATE TABLE stage_osm.raw_bus_lines (
 *   fid                          serial    PRIMARY KEY,
 *   osm_id                       varchar,  -- Unique, will cast to int
 *   oneway                       varchar,  -- Only values 'yes', 'no', null
 *   highway                      varchar,  -- Text values
 *   lanes                        varchar,  -- Will cast to int
 *   junction                     varchar,  -- Text values
 *   geom                         geometry(LINESTRING, 4326)
 * );
 * CREATE TABLE stage_osm.raw_tram_lines (
 *   fid                          serial    PRIMARY KEY,
 *   osm_id                       varchar,  -- Unique, will cast to int
 *   tram_segregation_physical    varchar,  -- Text values
 *   geom                         geometry(LINESTRING, 4326)
 * );
 */

CREATE TABLE stage_osm.combined_lines (
  osm_id                      bigserial           NOT NULL,
  sub_id                      smallint            NOT NULL DEFAULT 1,
  oneway                      text                NOT NULL,
  mode                        public.mode_type    NOT NULL,
  highway                     text,
  lanes                       smallint,
  tram_segregation_physical   text,
  geom                        geometry(LINESTRING, 3067),
  PRIMARY KEY (osm_id, sub_id)
);

CREATE INDEX combined_lines_geom_idx
  ON stage_osm.combined_lines
  USING GIST (geom);

CREATE INDEX ON stage_osm.combined_lines (mode);

CREATE OR REPLACE FUNCTION stage_osm.populate_combined_lines()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt     bigint;
BEGIN
  WITH
    bus_cast AS (
      SELECT
        osm_id::bigint                AS osm_id,
        CASE
          WHEN oneway = 'yes' THEN 'FT'
          WHEN oneway = 'no' THEN 'B'
          WHEN oneway IS NULL THEN 'B'
        END                           AS oneway,
        'bus'::public.mode_type       AS mode,
        highway::text                 AS highway,
        round(lanes::real)::smallint  AS lanes,  -- There may appear values like '1.8' ...
        NULL::text                    AS tram_segregation_physical,
        ST_Transform(geom, 3067)      AS geom
      FROM stage_osm.raw_bus_lines
    ),
    tram_cast AS (
      SELECT
        osm_id::bigint                    AS osm_id,
        'FT'::text                        AS oneway,
        'tram'::public.mode_type          AS mode,
        NULL::text                        AS highway,
        NULL::smallint                    AS lanes,
        tram_segregation_physical::text   AS tram_segregation_physical,
        ST_Transform(geom, 3067)          AS geom
      FROM stage_osm.raw_tram_lines
    )
  INSERT INTO stage_osm.combined_lines (
    osm_id, oneway, mode, highway, lanes, tram_segregation_physical, geom
  )
  SELECT * FROM bus_cast
  UNION
  SELECT * FROM tram_cast
  ON CONFLICT DO NOTHING;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into stage_osm.combined_lines', cnt;

  RETURN 'OK';
END;
$$;

/*
 * TODO: This function seems not to fix all the spots necessary, investigate!
 */
CREATE OR REPLACE FUNCTION stage_osm.fix_unconnected_combined_lines(
  drop_intermediate_tables  boolean   DEFAULT true
)
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt     bigint;
  s_info  text;
BEGIN
  DROP TABLE IF EXISTS stage_osm.unconnected_combined_lines;
  CREATE TABLE stage_osm.unconnected_combined_lines AS (
    SELECT
      f.osm_id                                    AS failing_link,
      t.osm_id                                    AS touching_link,
      f.mode                                      AS mode,
      f.geom                                      AS f_geom,
      t.geom                                      AS t_geom
    FROM stage_osm.combined_lines       AS f
    INNER JOIN stage_osm.combined_lines AS t
    /*
     * Note: ST_Touches is significantly faster than ST_Relate and is therefore
     *       used first to speed up the join. ST_Relate then narrows down
     *       the join condition so that we don't end up with symmetric pairs
     *       of failing and touching links.
     */
      ON ST_Touches(f.geom, t.geom)
      AND ST_Relate(f.geom, t.geom, 'F01FF0102')
      AND f.mode = t.mode
  );
  SELECT
    'Failing links to split: '
    || count(*) filter(WHERE mode = 'bus'::mode_type) || ' bus, '
    || count(*) filter(WHERE mode = 'tram'::mode_type) || ' tram'
  INTO s_info
  FROM stage_osm.unconnected_combined_lines;

  RAISE NOTICE '%', s_info;

  DROP TABLE IF EXISTS stage_osm.split_combined_lines;
  CREATE TABLE stage_osm.split_combined_lines AS (
    WITH
    all_split_points AS (
      SELECT
        failing_link,
        touching_link,
        f_geom,
        (ST_Dump(
          ST_Intersection(f_geom, t_geom)
        )).geom AS pt_geom
      FROM stage_osm.unconnected_combined_lines
      ORDER BY failing_link, touching_link
    ),
    distinct_split_points AS (
      SELECT DISTINCT ON (pt_geom) *
      FROM all_split_points
    ),
    multi_split_points AS (
      SELECT
        failing_link,
        f_geom,
        ST_Collect(pt_geom) AS pts_geom
      FROM distinct_split_points
      GROUP BY failing_link, f_geom
    ),
    split AS (
      SELECT
        failing_link,
        f_geom,
        (ST_Dump(
          ST_Split(f_geom, pts_geom)
        )).geom AS sp_geom
      FROM multi_split_points
    )
    SELECT
      failing_link,
      row_number() OVER (
        PARTITION BY failing_link) AS sub_id,
      f_geom,
      sp_geom
    FROM split
  );

  INSERT INTO stage_osm.combined_lines (
    osm_id, sub_id, oneway, mode, highway, lanes, tram_segregation_physical, geom
  )
  SELECT
    scl.failing_link              AS osm_id,
    scl.sub_id                    AS sub_id,
    cl.oneway                     AS oneway,
    cl.mode                       AS mode,
    cl.highway                    AS highway,
    cl.lanes                      AS lanes,
    cl.tram_segregation_physical  AS tram_segregation_physical,
    scl.sp_geom                   AS geom
  FROM stage_osm.split_combined_lines   AS scl
  INNER JOIN stage_osm.combined_lines   AS cl
    ON scl.failing_link = cl.osm_id
  ON CONFLICT ON CONSTRAINT combined_lines_pkey DO UPDATE
    SET geom = EXCLUDED.geom;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted or updated', cnt;

  IF drop_intermediate_tables THEN
    RAISE NOTICE 'Dropping intermediate tables';
    DROP TABLE stage_osm.unconnected_combined_lines;
    DROP TABLE stage_osm.split_combined_lines;
  END IF;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_osm.fix_unconnected_combined_lines IS
'Fix assumed topology errors in stage_osm.combined_lines
where the end of link A touches the face of link B,
while in fact link B should be split on that point.
Links of type B are split and they are assigned a sub_id;
split part with original sub_id is updated, others inserted as new records.
NOTE: This is assumed a one-time operation, such that in the beginning
all the sub_ids have the default value of 1.';
