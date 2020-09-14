\qecho Should be valid nodes:
INSERT INTO nw.nodes(nodeid, geom) VALUES (1, ST_SetSRID(ST_MakePoint(385443, 6671474), 3067));
INSERT INTO nw.nodes(nodeid, geom) VALUES (2, ST_SetSRID(ST_MakePoint(385403, 6671479), 3067));
INSERT INTO nw.nodes(nodeid, geom) VALUES (3, ST_SetSRID(ST_MakePoint(385103, 6671238), 3067));

\qecho Should be an INVALID node, outside HSL area:
INSERT INTO nw.nodes(geom) VALUES (ST_SetSRID(ST_MakePoint(226574, 6928728), 3067));

\qecho Should be an INVALID node, too close to an existing node:
INSERT INTO nw.nodes(geom) VALUES (ST_SetSRID(ST_MakePoint(385443.5, 6671474.5), 3067));

\qecho Should be a valid link:
INSERT INTO nw.links(linkid, inode, jnode, mode, oneway, geom)
  VALUES (1, 1, 2, 'bus', false, ST_SetSRID(ST_MakeLine(ST_MakePoint(385443, 6671474), ST_MakePoint(385403, 6671479)), 3067));

\qecho Update should result in an INVALID link, because of inode mismatch:
UPDATE nw.links SET inode = 3 WHERE linkid = 1;

\qecho Update should result in an INVALID link, because of jnode mismatch:
UPDATE nw.links SET jnode = 3 WHERE linkid = 1;
