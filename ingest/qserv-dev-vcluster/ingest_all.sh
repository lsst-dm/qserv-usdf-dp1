#!/bin/bash

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

DATABASE=dp1
DATABASE_OPT="--database=${DATABASE}"
VERBOSE_OPT="--verbose"
DEBUG_OPT=

APP=register-database
LOG=${LOG_DIR}/${APP}.log
echo "Register database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} ../${DATABASE}.json >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

 
APP=register-table
for TABLE in Object Source ForcedSource DiaObject DiaSource ForcedSourceOnDiaObject SSObject SSSource Visit CcdVisit CoaddPatches MPCORB; do
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
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}.urls >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-chunks
TABLE=Source
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest chunk contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}.urls >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-chunks
TABLE=ForcedSource
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest chunk contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}.urls >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-chunks
TABLE=DiaObject
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest chunk contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}.urls >& ${LOG}
if [ $? -ne 0 ] ; then 
  echo FAILED;
  exit 1;
fi

APP=async-contrib-chunks
TABLE=DiaSource
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest chunk contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}.urls >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-chunks
TABLE=ForcedSourceOnDiaObject
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest chunk contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../data/${TABLE}.urls >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-table
TABLE=SSObject
URL=$(cat ../data/${TABLE}.urls)
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest table contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-table
TABLE=SSSource
URL=$(cat ../data/${TABLE}.urls)
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest table contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-table
TABLE=Visit
URL=$(cat ../data/${TABLE}.urls)
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest table contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-table
TABLE=CcdVisit
URL=$(cat ../data/${TABLE}.urls)
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest table contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-table
TABLE=CoaddPatches
URL=$(cat ../data/${TABLE}.urls)
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest table contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-terminated-by=',' --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=async-contrib-table
TABLE=MPCORB
URL=$(cat ../data/${TABLE}.urls)
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo "Ingest table contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
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

APP=create-director-index
for TABLE in Object Source DiaObject; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo "Create director index on ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo FAILED;
    exit 1;
  fi;
done

APP=create-table-index
for TABLE in Object Source ForcedSource DiaObject DiaSource ForcedSourceOnDiaObject SSObject SSSource Visit CcdVisit CoaddPatches MPCORB; do
  for idx in $(ls ../indexes/ | grep "_${TABLE}_" | grep json); do
    LOG=${LOG_DIR}/${APP}-${idx::-5}.log;
    echo "Create table index ${idx::-5} -> ${LOG}";
    ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../indexes/${idx} >& ${LOG};
    if [ $? -ne 0 ] ; then
      echo FAILED;
      exit 1;
    fi;
  done;
done

APP=rebuild-row-counters
for TABLE in Object Source ForcedSource DiaObject DiaSource ForcedSourceOnDiaObject SSObject SSSource Visit CcdVisit CoaddPatches MPCORB; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo "Build row counter stats on ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo FAILED : ${LOG};
    exit 1;
  fi;
done

echo "DONE"

