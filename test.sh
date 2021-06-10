#!/bin/sh

set -u

docker build -t datarttu/sujuikodb:latest . && {
  docker-compose -f ./docker-compose.test.yml up --exit-code-from dataimporter_test
  status="$?"
  [ "${status}" -ne 0 ] && {
    docker-compose -f ./docker-compose.test.yml logs
  }
  docker-compose -f ./docker-compose.test.yml down
  exit "${status}"
}
