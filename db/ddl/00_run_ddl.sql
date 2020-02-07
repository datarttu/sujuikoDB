/*
Create sujuiko database and execute DDL scripts from scratch.
*/
\set ON_ERROR_STOP on
\timing on

CREATE DATABASE sujuiko;

\i 01_extensions_schemas_types.sql
\i 02_stage_schema.sql
\i 03_network_schema.sql
\i 04_schedule_schema.sql
\i 05_observation_schema.sql

\timing off
