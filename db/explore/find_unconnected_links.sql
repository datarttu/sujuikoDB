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

DROP TABLE IF EXISTS failing_tram_links;
CREATE TABLE failing_tram_links AS (
  WITH failing AS (
    SELECT
      a.linkid  AS failing_link,
      b.linkid  AS touching_link,
      a.geom    AS geom
    FROM nw.links       AS a
    INNER JOIN nw.links AS b
    ON ST_Touches(a.geom, b.geom)
    WHERE a.mode = 'tram'::mode_type
      AND b.mode = 'tram'::mode_type
      AND ST_Relate(a.geom, b.geom, 'F01FF0102')
  )
  SELECT
    failing_link,
    array_agg(touching_link ORDER BY touching_link) AS touching_links,
    geom
  FROM failing
  GROUP BY failing_link, geom
  ORDER BY failing_link
);

DROP TABLE IF EXISTS failing_bus_links;
CREATE TABLE failing_bus_links AS (
  WITH failing AS (
    SELECT
      a.linkid  AS failing_link,
      b.linkid  AS touching_link,
      a.geom    AS geom
    FROM nw.links       AS a
    INNER JOIN nw.links AS b
    ON ST_Touches(a.geom, b.geom)
    WHERE a.mode = 'bus'::mode_type
      AND b.mode = 'bus'::mode_type
      AND ST_Relate(a.geom, b.geom, 'F01FF0102')
  )
  SELECT
    failing_link,
    array_agg(touching_link ORDER BY touching_link) AS touching_links,
    geom
  FROM failing
  GROUP BY failing_link, geom
  ORDER BY failing_link
);

\timing off
