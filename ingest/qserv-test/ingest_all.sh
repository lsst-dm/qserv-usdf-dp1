#!/bin/bash

function TIMESTAMP {
  echo "[$(date +'%F %H:%M:%S')] "
}

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

rm -rf ${LOG_DIR}
mkdir -p ${LOG_DIR}

DATABASE=dp1
DATABASE_OPT="--database=${DATABASE}"
VERBOSE_OPT="--verbose"
DEBUG_OPT=
DIRECTOR_TABLES="Object Source DiaObject"
PARTITIONED_TABLES="Object Source ForcedSource DiaObject DiaSource ForcedSourceOnDiaObject"
FULLY_REPLICATED_TABLES="SSObject SSSource Visit CcdVisit ObsCore CoaddPatches MPCORB"
ALL_TABLES="${PARTITIONED_TABLES} ${FULLY_REPLICATED_TABLES}"

APP=register-database
LOG=${LOG_DIR}/${APP}.log
echo $(TIMESTAMP)"Register database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} ../${DATABASE}.json >& ${LOG}
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=register-table
for TABLE in ${ALL_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo $(TIMESTAMP)"Register table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../tables/${TABLE}.json >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=async-contrib-chunks
for TABLE in ${PARTITIONED_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log
  echo $(TIMESTAMP)"Ingest chunk contributions into ${TABLE} -> ${LOG}"
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}.urls >& ${LOG}
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=async-contrib-table
for TABLE in ${FULLY_REPLICATED_TABLES}; do
  URL=$(cat ../data/${TABLE}.urls)
  LOG=${LOG_DIR}/${APP}-${TABLE}.log
  echo $(TIMESTAMP)"Ingest table contributions into ${TABLE} -> ${LOG}"
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=publish-database
LOG=${LOG_DIR}/${APP}.log
echo $(TIMESTAMP)"Publish database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=create-director-index
for TABLE in ${DIRECTOR_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo $(TIMESTAMP)"Create director index on ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=create-table-index
for TABLE in ${ALL_TABLES}; do
  for idx in $(ls ../indexes/ | grep "_${TABLE}_" | grep json); do
    LOG=${LOG_DIR}/${APP}-${idx::-5}.log;
    echo $(TIMESTAMP)"Create table index ${idx::-5} -> ${LOG}";
    ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../indexes/${idx} >& ${LOG};
    if [ $? -ne 0 ] ; then
      echo $(TIMESTAMP)FAILED;
      exit 1;
    fi;
  done;
done

APP=rebuild-row-counters
for TABLE in ${ALL_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo $(TIMESTAMP)"Build row counter stats on ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

echo $(TIMESTAMP)DONE

