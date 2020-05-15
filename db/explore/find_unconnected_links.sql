/*
 * Try to find network links/edges that are not connected altough they should.
 * In these cases the end of edge 1 touches the face of edge 2,
 * while edge 2 should be split such that the ends would touch each other.
 */

\timing on
-- Tram link examples from Munkkiniemi.
/*
\qecho OK vs FAILING
SELECT ST_Relate(
  (SELECT geom FROM nw.links WHERE linkid = 9023),
  (SELECT geom FROM nw.links WHERE linkid = 1473)
);

\qecho FAILING vs OK
SELECT ST_Relate(
  (SELECT geom FROM nw.links WHERE linkid = 1473),
  (SELECT geom FROM nw.links WHERE linkid = 9023)
);

\qecho OK vs OK
SELECT ST_Relate(
  (SELECT geom FROM nw.links WHERE linkid = 9022),
  (SELECT geom FROM nw.links WHERE linkid = 9023)
);
*/
/*
BEGIN;

DROP TABLE IF EXISTS unconnected_osm_links;
CREATE TABLE unconnected_osm_links AS (
  WITH failing AS (
    SELECT
      a.osm_id  AS failing_link,
      b.osm_id  AS touching_link,
      a.mode    AS mode,
      a.geom    AS geom
    FROM stage_osm.combined_lines       AS a
    INNER JOIN stage_osm.combined_lines AS b
    ON ST_Touches(a.geom, b.geom)
    WHERE a.mode = b.mode
      AND ST_Relate(a.geom, b.geom, 'F01FF0102')
  )
  SELECT
    failing_link,
    mode,
    array_agg(touching_link ORDER BY touching_link) AS touching_links,
    geom
  FROM failing
  GROUP BY failing_link, mode, geom
  ORDER BY failing_link
);
*/

/*
 * 2020-05-15
 * Still having issues with some edges after successfully splitting others.
 */


SELECT ST_Touches(
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443800251),
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443762730)
);

-- -> touches YES

SELECT ST_Relate(
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443800251),
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443762730),
 'F01FF0102'
);

-- -> asymmetric touches NO

SELECT ST_Relate(
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443762730),
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443800251),
 'F01FF0102'
);

-- -> the other way around: NO

SELECT ST_Relate(
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443800251),
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443762730)
);

-- ->         'F01FFF102'
-- should be  'F01FF0102'

SELECT ST_Intersects(
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443800251),
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 443762730)
);

-- -> intersects YES

-- Example of a valid connection between 1) ring whose ends are attached to 2) edge end
SELECT ST_Relate(
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 186799306),
 (SELECT geom FROM stage_osm.combined_lines WHERE osm_id = 680591160)
);
-- ->         'F01FFF102'

-- Tram depot, feature B touches A with its both ends:
SELECT ST_Relate(
 (SELECT geom FROM stage_osm.raw_tram_lines WHERE fid = 824),
 (SELECT geom FROM stage_osm.raw_tram_lines WHERE fid = 67)
);
-- -> F01FF01F2

BEGIN;

CREATE TABLE dctesting AS (
  WITH failing AS (
    SELECT
      a.osm_id  AS failing_link,
      b.osm_id  AS touching_link,
      a.mode    AS mode,
      a.geom    AS geom
    FROM stage_osm.combined_lines       AS a
    INNER JOIN stage_osm.combined_lines AS b
    ON ST_Touches(a.geom, b.geom)
    WHERE a.mode = b.mode
      AND ST_Relate(a.geom, b.geom, 'F01FFF102')
  )
  SELECT
    failing_link,
    mode,
    array_agg(touching_link ORDER BY touching_link) AS touching_links,
    geom
  FROM failing
  GROUP BY failing_link, mode, geom
  ORDER BY failing_link
);

-- -> ALL of these failing links are LOOPS

\timing off
