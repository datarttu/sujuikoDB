/*
 * Create a new schema with a toy network
 * to demonstrate pgr_contraction.
 */
\o testnw_contraction.out
\set toler 0.00001
BEGIN;

DROP SCHEMA IF EXISTS sbnw CASCADE;

CREATE SCHEMA sbnw;

CREATE TABLE sbnw.testnw (
    id serial PRIMARY KEY,
    geom public.geometry(LineString, 4326),
    oneway text
);

COPY sbnw.testnw (id, oneway, geom) FROM stdin WITH CSV HEADER;
id,oneway,geom
1,B,0102000020E610000002000000E8D41DCF2AF238409CCA703842144E40FAF1A179CEF138400A441F0C40144E40
2,B,0102000020E610000002000000FAF1A179CEF138400A441F0C40144E400AB6A32F59F13840BB37D6753D144E40
3,B,0102000020E6100000020000000AB6A32F59F13840BB37D6753D144E407843588823F138403173F0373C144E40
4,B,0102000020E6100000020000007843588823F138403173F0373C144E40FC026DF5EFF03840103D062F3B144E40
5,B,0102000020E610000002000000FC026DF5EFF03840103D062F3B144E40E381DF78E8F03840896F167640144E40
10,B,0102000020E610000002000000FC026DF5EFF03840103D062F3B144E400A4F6A41D3F0384061BEBA543A144E40
11,B,0102000020E610000002000000555EC139B8F03840F5F6288739144E40737222A97FF03840FDBDC06338144E40
12,B,0102000020E6100000030000006B2D4ACD8DF0384098EE4AEF24144E40CAE7C145B9F03840E2DA463E37144E40555EC139B8F03840F5F6288739144E40
13,B,0102000020E6100000020000006B2D4ACD8DF0384098EE4AEF24144E408DF3AF2587F03840B8D49BCB20144E40
14,B,0102000020E6100000020000008DF3AF2587F03840B8D49BCB20144E40E25FC5CA84F03840E9593D1A1F144E40
18,FT,0102000020E610000003000000737222A97FF03840DE70036A1B144E403B5A334778F0384099F8208B1B144E4052C6E45921F03840B8BEE5EB17144E40
19,FT,0102000020E61000000200000052C6E45921F03840B8BEE5EB17144E4089BEAAC61CF038408F4083181D144E40
20,FT,0102000020E6100000020000004C77B7F11BF03840FE62E5A51E144E4099A1DD7219F038401B7BBD3C27144E40
21,FT,0102000020E61000000400000099A1DD7219F038401B7BBD3C27144E40094EC3C155F038408CB7A0AF28144E404C3D66F374F03840376E3B5727144E406B2D4ACD8DF0384098EE4AEF24144E40
22,FT,0102000020E61000000200000099A1DD7219F038401B7BBD3C27144E40F84B5F1FE7EF3840F9ACC5D226144E40
23,FT,0102000020E61000000200000089BEAAC61CF038408F4083181D144E404C77B7F11BF03840FE62E5A51E144E40
24,FT,0102000020E6100000020000002858CFA409F0384051477F4D1D144E4089BEAAC61CF038408F4083181D144E40
25,FT,0102000020E610000004000000E381DF78E8F03840896F167640144E404F30EF17F6F03840CF92898E42144E40FC1973AF1FF138403555A99852144E40543B76F540F13840FE3DF8F15F144E40
26,FT,0102000020E610000002000000543B76F540F13840FE3DF8F15F144E4086E40EFA36F13840DCD4CE317C144E40
27,FT,0102000020E61000000200000086E40EFA36F13840DCD4CE317C144E40F5D49AD32DF138408F863FC990144E40
28,FT,0102000020E610000002000000F5D49AD32DF138408F863FC990144E40DA530D5726F13840A560DD8294144E40
29,B,0102000020E610000002000000DA530D5726F13840A560DD8294144E40F27B18DF14F13840B624B5D29A144E40
30,FT,0102000020E610000002000000DA530D5726F13840A560DD8294144E40B0EF4C2E22F138409CA35AC08F144E40
31,FT,0102000020E610000002000000B0EF4C2E22F138409CA35AC08F144E402CCD604029F1384034161C1779144E40
32,FT,0102000020E6100000030000002CCD604029F1384034161C1779144E40CEBF07132FF13840754F758865144E40B88DA7FE2CF138402770B9A262144E40
33,FT,0102000020E610000003000000B88DA7FE2CF138402770B9A262144E40C970DAAA29F138403D6DF32660144E40CD03AE9DE9F03840848CF57C43144E40
34,FT,0102000020E610000002000000CD03AE9DE9F03840848CF57C43144E40E381DF78E8F03840896F167640144E40
35,B,0102000020E610000002000000543B76F540F13840FE3DF8F15F144E40244E376690F138401310547F61144E40
40,B,0102000020E610000002000000244E376690F138401310547F61144E4047EE128258F238405201C58B65144E40
41,B,0102000020E61000000200000070F950B643F238400A1FCF7854144E40E8D41DCF2AF238409CCA703842144E40
42,B,0102000020E61000000200000047EE128258F238405201C58B65144E4070F950B643F238400A1FCF7854144E40
43,B,0102000020E61000000200000047EE128258F238405201C58B65144E40DB1CB89ED3F238407F88FFD26D144E40
44,B,0102000020E6100000020000000A4F6A41D3F0384061BEBA543A144E40555EC139B8F03840F5F6288739144E40
45,B,0102000020E610000002000000E25FC5CA84F03840E9593D1A1F144E40737222A97FF03840DE70036A1B144E40
\.

ALTER TABLE sbnw.testnw ADD COLUMN source integer;
ALTER TABLE sbnw.testnw ADD COLUMN target integer;
ALTER TABLE sbnw.testnw ADD COLUMN cost double precision;
UPDATE sbnw.testnw SET cost = ST_Length(geom);
ALTER TABLE sbnw.testnw ADD COLUMN reverse_cost double precision DEFAULT -1;
UPDATE sbnw.testnw SET reverse_cost = cost WHERE oneway LIKE 'B';

SELECT pgr_createTopology('sbnw.testnw', :toler, 'geom');

SELECT pgr_analyzeGraph('sbnw.testnw', :toler, 'geom');

SELECT pgr_analyzeOneway(
  'sbnw.testnw',
  ARRAY['', 'B', 'TF'],
  ARRAY['', 'B', 'FT'],
  ARRAY['', 'B', 'FT'],
  ARRAY['', 'B', 'TF']
);

CREATE TABLE sbnw.nested_contracted AS (
  SELECT *
  FROM pgr_contraction(
    'SELECT id, source, target,
    cost, reverse_cost
    FROM sbnw.testnw',
    ARRAY[2],
    max_cycles := 1
  )
);
\qecho Nested contraction result
SELECT * FROM sbnw.nested_contracted;

/*
Note that two-way edges are represented twice in contracted vertices.
Therefore we use DISTINCT to get rid of too many merging groups
for same edges.
WARNING: This algorithm leads to erroneous merges if there are multiple
edges that use the same nodes but are modeled as one-way edges
in different directions, instead of one two-way edge.
As far as I know, there should not be such cases in our OSM subset.
*/
CREATE TABLE sbnw.edges_to_merge AS (
WITH
  distinct_contracted AS (
    SELECT DISTINCT ON (contracted_vertices) *
    FROM sbnw.nested_contracted
  ),
  unnested AS (
    SELECT
      id AS grp,
      unnest(source || contracted_vertices || target) AS vertex
    FROM distinct_contracted
  ),
  vertex_permutations_by_grp AS (
    SELECT u1.grp AS grp, u1.vertex AS source, u2.vertex AS target
    FROM unnested AS u1, unnested AS u2
    WHERE u1.grp = u2.grp AND u1.vertex <> u2.vertex
  )
SELECT
  n.id, vp.grp, n.source, n.target
FROM sbnw.testnw AS n
INNER JOIN vertex_permutations_by_grp AS vp
ON n.source = vp.source AND n.target = vp.target
GROUP BY vp.grp, n.id
);

\qecho Edges to merge
SELECT * FROM sbnw.edges_to_merge;

CREATE TABLE sbnw.all_edges_before_merging AS (
  SELECT
    orig.id,
    orig.source,
    orig.target,
    orig.oneway,
    coalesce(ctr.grp, orig.id) AS merge_group,
    orig.geom
  FROM sbnw.testnw AS orig
  LEFT JOIN sbnw.edges_to_merge AS ctr
  ON orig.source = ctr.source AND orig.target = ctr.target
);
\qecho All edges before merging
SELECT id, source, target, oneway, merge_group, ST_Length(geom)
FROM sbnw.all_edges_before_merging
ORDER BY merge_group, id;

CREATE TABLE sbnw.contracted_nw AS (
  SELECT
    merge_group AS id,
    NULL::bigint AS source,
    NULL::bigint AS target,
    min(oneway) AS oneway,
    ST_LineMerge(ST_Collect(geom)) AS geom
  FROM sbnw.all_edges_before_merging
  GROUP BY merge_group
);

\qecho Contracted edges
SELECT id, source, target, oneway, ST_Length(geom)
FROM sbnw.contracted_nw
ORDER BY id;

SELECT pgr_createTopology('sbnw.contracted_nw', :toler, 'geom');

COMMIT;
\o