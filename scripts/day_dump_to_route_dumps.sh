#!/bin/bash
#
# Read from arg 1 a csv file containing HFP data for a single oday,
# named like hfp_[yyyy-mm-dd].csv,
# and export into csv files per route,
# named like hfp_[yyyy-mm-dd]/route_[route].csv
# (or .../route_.csv if route is missing).
# Assuming the data looks like this:
# is_ongoing,event_type,dir,oper,veh,tst,lat,long,odo,drst,oday,start,loc,stop,route
# t,BA,,18,2833,2019-11-01 22:30:10+00,60.171941,24.94372,45117,f,2019-11-02,,GPS,,
# t,BA,,45,1239,2019-11-01 23:15:47+00,60.264479,25.08392,2842,t,2019-11-02,,GPS,,
# ...

set -e

infile="$1"
outdir="${infile%%.csv*}"

mkdir -p "$outdir"

awk -v outd="$outdir" -F ',' 'NR>1 {print > (outd "/route_" $15 ".csv")}' "$infile"

# NOTE: Result files do not have csv headers!
