#!/usr/bin/env bash
# Import TRAM network from .osm file to sujuiko database.
# Requires ogr2ogr as well as configuration .ini file tailored for the tram network.

osmfile="../data/rawdata/osm/tram_nw.osm"
configfile="../config/osmconfig_tram.ini"

ogr2ogr -f PostgreSQL \
  PG:"dbname='sujuiko' host='localhost' port='5435' user='postgres'" \
  "$osmfile" lines \
  --config OSM_USE_CUSTOM_INDEXING NO \
  -oo CONFIG_FILE="$configfile" \
  -lco "OVERWRITE=YES" \
  -lco "DIM=2" \
  -lco "GEOMETRY_NAME=geom" \
  -lco "SCHEMA=stage_osm" \
  -lco "FID=fid" \
  -lco "SPATIAL_INDEX=GIST" \
  -nln "raw_tram_lines"
