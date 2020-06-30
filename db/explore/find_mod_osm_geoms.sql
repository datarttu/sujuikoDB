/*
 * Find modified OSM geometries by comparing raw and combined_lines data.
 */

CREATE TABLE public.modified_osm_ways AS (
WITH
  bus_tf AS (
    SELECT
      osm_id::bigint,
      CASE
        WHEN oneway = 'yes' THEN 'FT'
        WHEN oneway IS NULL THEN 'B'
        ELSE 'B'
      END               AS oneway,
      'bus'::mode_type  AS mode,
      ST_Transform(geom, 3067)  AS geom
    FROM stage_osm.raw_bus_lines
  ),
  tram_tf AS (
    SELECT
      osm_id::bigint,
      'FT'              AS oneway,
      'tram'::mode_type AS mode,
      ST_Transform(geom, 3067)  AS geom
    FROM stage_osm.raw_tram_lines
  ),
  combo AS (
    SELECT * FROM bus_tf
    UNION
    SELECT * FROM tram_tf
  ),
  combined_lines_sel AS (
    SELECT
      osm_id,
      oneway,
      mode,
      geom
    FROM stage_osm.combined_lines
  )
SELECT
  cl.osm_id,
  cl.geom AS new_geom,
  cb.geom AS old_geom
FROM combined_lines_sel       AS cl
INNER JOIN combo              AS cb
  ON  cl.osm_id = cb.osm_id
  -- No other differences found than geom-related.
  AND cl.geom IS DISTINCT FROM cb.geom
UNION
SELECT
  osm_id,
  geom    AS new_geom,
  NULL::geometry(LINESTRING, 3067)  AS old_geom
FROM stage_osm.combined_lines
WHERE osm_id = 0
);
CREATE INDEX ON public.modified_osm_ways USING GIST (new_geom);
CREATE INDEX ON public.modified_osm_ways USING GIST (old_geom);

SELECT
  format(
    $f$UPDATE stage_osm.combined_lines SET geom = '%s' WHERE osm_id = %s;$f$,
    new_geom,
    osm_id
  ) AS cmd
FROM public.modified_osm_ways
WHERE osm_id > 0;

SELECT
  format(
    $f$(0, '%s', '%s'::mode_type, '%s'),$f$,
    oneway,
    mode,
    geom
  ) AS fmt
FROM stage_osm.combined_lines
WHERE osm_id = 0;
