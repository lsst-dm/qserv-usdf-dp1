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

DATABASE=ivoa
DATABASE_OPT="--database=${DATABASE}"
VERBOSE_OPT="--verbose"
DEBUG_OPT=
TABLE=ObsCore

APP=unpublish-database
LOG=${LOG_DIR}/${APP}.log
echo $(TIMESTAMP)"Unpublish database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=delete-table
LOG=${LOG_DIR}/${APP}-${TABLE}.log;
echo $(TIMESTAMP)"Delete table ${TABLE} -> ${LOG}";
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG};
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=register-table
LOG=${LOG_DIR}/${APP}-${TABLE}.log;
echo $(TIMESTAMP)"Register table ${TABLE} -> ${LOG}";
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../tables/${TABLE}.json >& ${LOG};
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=async-contrib-table
URL=$(cat ../data/${TABLE}.urls)
LOG=${LOG_DIR}/${APP}-${TABLE}.log
echo $(TIMESTAMP)"Ingest table contributions into ${TABLE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} --fields-enclosed-by='"' --fields-terminated-by=',' ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=publish-database
LOG=${LOG_DIR}/${APP}.log
echo $(TIMESTAMP)"Publish database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG}
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=create-table-index
for idx in $(ls ../indexes/ | grep "_${TABLE}_" | grep json); do
  LOG=${LOG_DIR}/${APP}-${idx::-5}.log;
  echo $(TIMESTAMP)"Create table index ${idx::-5} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../indexes/${idx} >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=rebuild-row-counters
LOG=${LOG_DIR}/${APP}-${TABLE}.log;
echo $(TIMESTAMP)"Build row counter stats on ${TABLE} -> ${LOG}";
../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} >& ${LOG};
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

echo $(TIMESTAMP)DONE

