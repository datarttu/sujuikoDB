/*
 * This is a temporary testing solution:
 * run this inside the Docker container with psql as user "postgres".
 * You should be able to run this without error messages.
 */
CREATE DATABASE test_sujuiko;
\c test_sujuiko
CREATE EXTENSION postgis;
\i /db/schema_nw.sql
COPY nw.view_node_wkt FROM '/data/node.csv' CSV HEADER;
COPY nw.view_link_wkt FROM '/data/link.csv' CSV HEADER;
\c postgres
DROP DATABASE test_sujuiko;
