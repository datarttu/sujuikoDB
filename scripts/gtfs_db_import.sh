#!/usr/bin/env bash
# Import GTFS tables to db staging schema.
# Note that we create intermediate file(s)
# to fix some bugs in HSL GTFS datasets:
# - Whitespaces ", ," in stops.txt:parent_station field

set -e
source "../.env"

gtfs_dir=${GTFS_DIR:-"../data/rawdata/gtfs/gtfs_20191101"}
connstring=${PG_CONN:-"dbname='sujuiko' host='localhost' port='5435' user='postgres'"}

[[ -d "$gtfs_dir" ]] || (echo "$gtfs_dir"" does not exist!" && exit 1)

# Fixing source data bugs here
sed 's/, ,/,,/g' "$gtfs_dir""/stops.txt" > "$gtfs_dir""/stops_tmp.txt"

psql -d "$connstring" \
  --set ON_ERROR_STOP=on \
  --set AUTOCOMMIT=off << EOF
BEGIN;
TRUNCATE TABLE stage_gtfs.routes;
TRUNCATE TABLE stage_gtfs.calendar;
TRUNCATE TABLE stage_gtfs.calendar_dates;
TRUNCATE TABLE stage_gtfs.stops;
TRUNCATE TABLE stage_gtfs.trips;
TRUNCATE TABLE stage_gtfs.shapes;
TRUNCATE TABLE stage_gtfs.stop_times;
\timing on
\echo routes
\copy stage_gtfs.routes FROM '$gtfs_dir/routes.txt' WITH CSV HEADER;
\echo calendar
\copy stage_gtfs.calendar FROM '$gtfs_dir/calendar.txt' WITH CSV HEADER;
\echo calendar_dates
\copy stage_gtfs.calendar_dates FROM '$gtfs_dir/calendar_dates.txt' WITH CSV HEADER;
\echo stops
\copy stage_gtfs.stops FROM '$gtfs_dir/stops_tmp.txt' WITH CSV HEADER;
\echo trips
\copy stage_gtfs.trips FROM '$gtfs_dir/trips.txt' WITH CSV HEADER;
\echo shapes
\copy stage_gtfs.shapes FROM '$gtfs_dir/shapes.txt' WITH CSV HEADER;
\echo stop_times
\copy stage_gtfs.stop_times FROM '$gtfs_dir/stop_times.txt' WITH CSV HEADER;
\timing off
COMMIT;
EOF

rm "$gtfs_dir""/stops_tmp.txt"
