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
