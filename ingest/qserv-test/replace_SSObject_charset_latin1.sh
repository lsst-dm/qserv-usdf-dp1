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

APP=unpublish-database
LOG=${LOG_DIR}/${APP}.log
echo "Unpublish database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

#if [ 0 -ne 0 ] ; then
APP=delete-table
for TABLE in SSObject; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log
  echo "Delete table ${TABLE} -> ${LOG}"
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
  if [ $? -ne 0 ] ; then
    echo FAILED;
    exit 1;
  fi;
done
#fi
 
APP=register-table
# CHARSET_COLLATION_OPT="--charset=utf8mb4 --collation=utf8mb4_uca1400_ai_ci"
CHARSET_COLLATION_OPT="--charset=latin1"
for TABLE in SSObject; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo "Register table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${CHARSET_COLLATION_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} ../tables/${TABLE}.json >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo FAILED;
    exit 1;
  fi;
done


APP=async-contrib-table
for TABLE in SSObject; do
  URL=$(cat ../data/${TABLE}.urls)
  LOG=${LOG_DIR}/${APP}-${TABLE}.log
  echo "Ingest table contributions into ${TABLE} -> ${LOG}"
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
  if [ $? -ne 0 ] ; then
    echo FAILED;
    exit 1;
  fi;
done

APP=publish-database
LOG=${LOG_DIR}/${APP}.log
echo "Publish database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo FAILED;
  exit 1;
fi

APP=create-table-index
for TABLE in SSObject; do
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
for TABLE in SSObject; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo "Build row counter stats on ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo FAILED : ${LOG};
    exit 1;
  fi;
done

echo "DONE"

