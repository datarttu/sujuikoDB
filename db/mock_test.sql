-- Should be a valid node:
INSERT INTO nw.nodes(geom) VALUES (ST_SetSRID(ST_MakePoint(385443, 6671474), 3067));

-- Should be an INVALID node, outside HSL area:
INSERT INTO nw.nodes(geom) VALUES (ST_SetSRID(ST_MakePoint(226574, 6928728), 3067));

-- Should be an INVALID node, too close to an existing node:
INSERT INTO nw.nodes(geom) VALUES (ST_SetSRID(ST_MakePoint(385443.5, 6671474.5), 3067));
