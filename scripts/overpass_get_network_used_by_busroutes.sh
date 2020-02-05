#!/usr/bin/env bash
# Get ways in HSL area used by bus route relations
# as they were at "date".
endpoint="https://overpass-api.de/api/interpreter"
req_body='
[out:xml][date:"2019-11-30T00:00:00Z"];
area
  ["boundary"="administrative"]
  ["name"="Helsingin seutukunta"]->.a;
rel(area.a)["route"="bus"];
way(r)(area.a);
out body; >; out skel qt;
'
target_dir="../data/rawdata/osm"
mkdir -p "$target_dir"
target_path="$target_dir""/bus_rel_nw.osm"
wget -O "$target_path" --post-data="$req_body" "$endpoint"
