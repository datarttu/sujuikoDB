/*
 * Test DDL scripts on a database that is eventually dropped.
 * If you get just NOTICEs / other prints but no ERRORs,
 * then the DDL scripts should work just fine.
 * Will drop any existing db with the same name!!
 */

\c postgres
DROP DATABASE IF EXISTS test_sujuiko;
CREATE DATABASE test_sujuiko;
\c test_sujuiko

BEGIN;
\i RUN_DDL.sql
COMMIT;

\c postgres
DROP DATABASE test_sujuiko;
