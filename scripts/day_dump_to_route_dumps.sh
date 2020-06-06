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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S%:z')] $1"
}
LOG_FILE="$DIR""/dumps.log"
exec 1>>"$LOG_FILE"
exec 2>&1

infile="$1"
outdir="${infile%%.csv*}""_routes"

log "Start $infile -> $outdir"

mkdir -p "$outdir"

gzip -cd "$infile" | awk -v outd="$outdir" -F ',' 'NR>1 {print > (outd "/route_" $15 ".csv")}'
rm "$outdir""/route_.csv"
cd "$outdir"
find . -type f -name '*.csv' -exec gzip "{}" \;
cd -

log "End $infile -> $outdir"
