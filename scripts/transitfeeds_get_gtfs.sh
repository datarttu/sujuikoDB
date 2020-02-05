#!/usr/bin/env bash
# Get HSL GTFS dataset valid from November 1st on.
datefrom="20191101"
endpoint="https://transitfeeds.com/p/helsinki-regional-transport/735/""$datefrom""/download"
target_dir="../data/rawdata/gtfs"
mkdir -p "$target_dir"
target_path="$target_dir""/gtfs_""$datefrom"
zip_path="$target_path"".zip"
# Exit if GTFS dir in question is already available.
if [[ -d "$target_path" ]]; then
  echo "$target_path"" already exists"
  exit 0
fi
# If zip in question is available, skip downloading.
if [[ ! -f "$zip_path" ]]; then
  wget -O "$zip_path" "$endpoint"
else
  echo "$zip_path"" already downloaded"
fi
unzip "$zip_path" -d "$target_path" && rm "$zip_path"
