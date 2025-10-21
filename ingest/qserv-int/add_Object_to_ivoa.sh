#!/bin/bash
#
# This scenario is meant for adding the dummy director table 'Object' that was accientally
# removed from database 'ivoa'. Added a single chunk 84482. Otherwise the database wouldn't
# be published.

BASE_DIR=$(dirname "$0")
if [ -z "$BASE_DIR" ] || [ "$0" = "bash" ]; then
    >&2 echo "error: variable 'BASE_DIR' is not defined"
    return 1
fi
BASE_DIR=$(readlink -e "$BASE_DIR")
if [ ! -d "$BASE_DIR" ]; then
    >&2 echo "error: path 'BASE_DIR' is not a valid directory"
    return 1
fi
LOG_DIR=${BASE_DIR}/logs
cd ${BASE_DIR}

rm -rf logs/
mkdir -p logs

DATABASE=ivoa
DATABASE_OPT="--database=${DATABASE}"
VERBOSE_OPT="--verbose"
DEBUG_OPT=

APP=unpublish-database
LOG=${LOG_DIR}/${APP}.log
echo "Unpublish database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=set-ingest-config
LOG=${LOG_DIR}/${APP}.log
echo "Configure ingest for database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} ../${DATABASE}_ingest_config.json>& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=register-table
for TABLE in Object; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo "Register table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../tables/${TABLE}.json >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo FAILED;
    exit 1;
  fi;
done

APP=async-contrib-chunks
TABLE=Object
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest chunk contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}-chunk-84482.urls >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=publish-database
LOG=${LOG_DIR}/${APP}.log
echo "Publish database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

echo "DONE"

