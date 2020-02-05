#!/usr/bin/env bash
# Get HSL tram network ways and nodes as they were at "date" from Overpass API.
endpoint="https://overpass-api.de/api/interpreter"
req_body='
[out:xml][date:"2019-11-30T00:00:00Z"];
area
  ["boundary"="administrative"]
  ["name"="Helsingin seutukunta"]->.a;
way(area.a)["railway"="tram"];
out body; >; out skel qt;
'
target_dir="../data/rawdata/osm"
mkdir -p "$target_dir"
target_path="$target_dir""/tram_nw.osm"
wget -O "$target_path" --post-data="$req_body" "$endpoint"
