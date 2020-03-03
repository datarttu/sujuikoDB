/*
 * Create tables for the OSM staging schema.
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\c sujuiko;

BEGIN;
\echo Creating stage_osm schema ...

CREATE SCHEMA IF NOT EXISTS stage_osm;

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
  osm_id                      bigserial           PRIMARY KEY,
  oneway                      text                NOT NULL,
  mode                        public.mode_type    NOT NULL,
  highway                     text,
  lanes                       smallint,
  tram_segregation_physical   text,
  geom                        geometry(LINESTRING, 3067)
);

CREATE INDEX combined_lines_geom_idx
  ON stage_osm.combined_lines
  USING GIST (geom);

CREATE OR REPLACE FUNCTION stage_osm.populate_combined_lines()
RETURNS TABLE (
  mode            public.mode_type,
  rows_inserted   bigint
)
LANGUAGE SQL
VOLATILE
AS $$
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
  ),
  combined_insert AS (
    INSERT INTO stage_osm.combined_lines
    SELECT * FROM bus_cast
    UNION
    SELECT * FROM tram_cast
    ON CONFLICT DO NOTHING
    RETURNING mode
  )
  SELECT
    mode,
    count(mode) AS rows_inserted
  FROM combined_insert
  GROUP BY mode;
  -- TODO: SELECT these into a logging table
$$;

COMMIT;
