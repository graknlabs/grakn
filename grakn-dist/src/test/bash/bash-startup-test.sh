#!/bin/bash

# Requirements
# shunit2
# distribution tar under target

BASEDIR=$(dirname "$0")
GRAKN_DIST_TARGET=$BASEDIR/../../../target/
GRAKN_DIST_TMP=$GRAKN_DIST_TARGET/grakn-bash-test/
REDIS_DATA_DIR=./db/redis

startEngine(){
  echo "Starting!"
  _JAVA_OPTIONS="-Dcassandra.log.appender=STDOUT" "${GRAKN_DIST_TMP}/grakn" server start
}

loadData(){
  echo "Inserting data!"
  "${GRAKN_DIST_TMP}"/graql console < insert-data.gql
}

oneTimeSetUp() {
  set -e
  package=$(ls $GRAKN_DIST_TARGET/grakn-dist-*.tar.gz | head -1)
  mkdir -p ${GRAKN_DIST_TMP}
  mkdir -p ${REDIS_DATA_DIR}
  tar -xf $GRAKN_DIST_TARGET/$(basename ${package}) -C ${GRAKN_DIST_TMP} --strip-components=1

  startEngine
  loadData
  set +e
}

oneTimeTearDown() {
  echo "y" | "${GRAKN_DIST_TMP}"/grakn server clean
  "${GRAKN_DIST_TMP}"/grakn server stop
}

testPersonCount()
{
  PERSON_COUNT=$(cat query-data.gql | "${GRAKN_DIST_TMP}"/graql console | grep -v '>>>' | grep person | wc -l)
  echo Persons found $PERSON_COUNT
  assertEquals 4 $PERSON_COUNT
}

testMarriageCount()
{
  MARRIAGE_COUNT=$(cat query-marriage.gql | "${GRAKN_DIST_TMP}"/graql console |  grep -v '>>>' | grep person | wc -l)
  echo Marriages found $MARRIAGE_COUNT
  assertEquals 1 $MARRIAGE_COUNT
}

# Source shunit2
source "$GRAKN_DIST_TARGET"/lib/shunit2-master/source/2.1/src/shunit2
