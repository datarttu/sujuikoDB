/*
Create sujuiko database and execute DDL scripts from scratch.

Arttu K 2020-02
*/
\set ON_ERROR_STOP on

CREATE DATABASE sujuiko;

BEGIN;
\i 01_extensions_global.sql
\i 02_stage_schema.sql
\i 03_network_schema.sql
\i 04_schedule_schema.sql
\i 05_observation_schema.sql
COMMIT;
