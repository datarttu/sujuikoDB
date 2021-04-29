#!/bin/sh

set -u

docker build -t datarttu/sujuikodb:latest .
docker-compose -f ./docker-compose.test.yml up
status="$?"
[ "${status}" -ne 0 ] && {
  docker-compose -f ./docker-compose.test.yml logs
}
exit "${status}"
