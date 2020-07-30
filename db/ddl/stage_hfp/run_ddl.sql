/*
 * stage_hfp DDL scripts.
 * In production, you can use main level RUN_DDL.sql.
 * This leaves the transaction open so you must commit manually if needed!
 */

BEGIN;

\ir raw.sql
\ir triggers.sql
\ir journeys.sql
\ir logging.sql
\ir jrn_points.sql
\ir jrn_segs.sql
