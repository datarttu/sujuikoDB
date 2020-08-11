#!/bin/bash
# Call HFP import procedure with multiple datasets.

routes=("1039" "1040" "1041")
for route in "${routes[@]}"; do
  for d in {01..30}; do
    oday="2019-11-""$d"
    template="/data0/hfpdumps/november/hfp_%s_routes/route_%s.csv.gz"
    sql_cmd="
    BEGIN;
    CALL stage_hfp.import_dump(
      route             := '$route',
      oday              := '$oday',
      gz_path_template  := '$template'
    );
    COMMIT;
    "
    psql -h ${PG_HOST:-"localhost"} -p ${PG_PORT:-"5432"} -d ${PG_DB:-"sujuiko"} -U ${PG_USER:-"postgres"} -c "$sql_cmd"
  done
done
