#!/bin/bash
# Call HFP import procedure with a single dataset.
# Param 1: route identifier
# Param 2: operating day as yyyy-mm-dd

template="/data0/hfpdumps/november/hfp_%s_routes/route_%s.csv.gz"
sql_cmd="
BEGIN;
CALL stage_hfp.import_dump(
  route             := '$1',
  oday              := '$2',
  gz_path_template  := '$template'
);
COMMIT;
"
echo "$PG_HOST"
echo "$PG_PORT"
echo "$PG_DB"
echo "$PG_USER"
psql -h ${PG_HOST:-"localhost"} -p ${PG_PORT:-"5432"} -d ${PG_DB:-"sujuiko"} -U ${PG_USER:-"postgres"} -c "$sql_cmd"
