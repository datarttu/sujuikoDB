/*
Create sujuiko database and execute DDL scripts from scratch.
*/
\set ON_ERROR_STOP on
\timing on

CREATE DATABASE sujuiko;

\i 00_extensions_schemas_types.sql
\i 01_stage_schema.sql
\i 02_network_schema.sql
\i 03_schedule_schema.sql
\i 04_observation_schema.sql

\timing off
