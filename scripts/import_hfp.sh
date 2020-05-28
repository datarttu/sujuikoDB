#!/usr/bin/env bash
# Import a day-route HFP dump to stage_hfp schema.
# Argument 1: Path of csv file to import (on the server and readable by postgres user).

set -e

connstring=${PG_CONN:-"dbname='sujuiko' host='localhost' port='5432' user='postgres'"}
csvpath="$1"
sql_statement="
COPY stage_hfp.raw (
  is_ongoing,
  event_type,
  dir,
  oper,
  veh,
  tst,
  lat,
  lon,
  odo,
  drst,
  oday,
  start,
  loc,
  stop,
  route
)
FROM '$csvpath'
WITH CSV;
"
# echo "$sql_statement"
psql -d "$connstring" -c "$sql_statement"
