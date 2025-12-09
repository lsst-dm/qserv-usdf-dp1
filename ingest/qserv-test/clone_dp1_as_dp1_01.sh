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

# Variables that define a scope of the ingest
SOURCE_DATABASE=dp1
SOURCE_DATABASE_OPT="--database=${SOURCE_DATABASE}"
SOURCE_CONFIG_OPT="--qserv=source-qserv.json"
DATABASE=dp1_01
DATABASE_OPT="--database=${DATABASE}"
VERBOSE_OPT="--verbose"
DEBUG_OPT=
DIRECTOR_TABLES="Object Source DiaObject"
DEPENDENT_TABLES="ForcedSource DiaSource ForcedSourceOnDiaObject"
PARTITIONED_TABLES="Object Source ForcedSource DiaObject DiaSource ForcedSourceOnDiaObject"
FULLY_REPLICATED_TABLES="SSObject SSSource Visit CcdVisit ObsCore CoaddPatches MPCORB"
ALL_TABLES="${PARTITIONED_TABLES} ${FULLY_REPLICATED_TABLES}"

# CSV dialect definitions for the tables
Object_CSV_DIALECT=
Source_CSV_DIALECT=
ForcedSource_CSV_DIALECT=
DiaObject_CSV_DIALECT=
DiaSource_CSV_DIALECT='--fields-enclosed-by="'
ForcedSourceOnDiaObject_CSV_DIALECT=
SSObject_CSV_DIALECT='--fields-enclosed-by="'
SSSource_CSV_DIALECT='--fields-enclosed-by="'
Visit_CSV_DIALECT='--fields-enclosed-by="'
CcdVisit_CSV_DIALECT='--fields-enclosed-by="'
ObsCore_CSV_DIALECT='--fields-enclosed-by=" --fields-terminated-by=,'
CoaddPatches_CSV_DIALECT='--fields-enclosed-by=" --fields-terminated-by=,'
MPCORB_CSV_DIALECT='--fields-enclosed-by="'

INDIR=${SOURCE_DATABASE}
rm -rf ${INDIR}
mkdir -p ${INDIR}/tables
mkdir -p ${INDIR}/data
mkdir -p ${INDIR}/indexes

APP=export-database-config
LOG=${LOG_DIR}/${APP}.log;
echo $(TIMESTAMP)"Export configuration of database ${SOURCE_DATABASE} -> ${LOG}";
../tools/${APP}.py ${SOURCE_CONFIG_OPT} ${SOURCE_DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/${SOURCE_DATABASE}.json >& ${LOG};
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi

APP=export-table-config
for TABLE in ${ALL_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo $(TIMESTAMP)"Export configuration of table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${SOURCE_CONFIG_OPT} ${SOURCE_DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/tables/${TABLE}.json >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=export-chunks
for TABLE in ${DIRECTOR_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  CSV_DIALECT="${TABLE}_CSV_DIALECT"
  echo $(TIMESTAMP)"Export chunks of table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${SOURCE_CONFIG_OPT} ${SOURCE_DATABASE_OPT} --table=${TABLE} --director ${!CSV_DIALECT} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/data/${TABLE}.urls >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done
for TABLE in ${DEPENDENT_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  CSV_DIALECT="${TABLE}_CSV_DIALECT"
  echo $(TIMESTAMP)"Export chunks of table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${SOURCE_CONFIG_OPT} ${SOURCE_DATABASE_OPT} --table=${TABLE} ${!CSV_DIALECT} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/data/${TABLE}.urls >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=export-table
for TABLE in ${FULLY_REPLICATED_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  CSV_DIALECT="${TABLE}_CSV_DIALECT"
  echo $(TIMESTAMP)"Export table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${SOURCE_CONFIG_OPT} ${SOURCE_DATABASE_OPT} --table=${TABLE} ${!CSV_DIALECT} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/data/${TABLE}.urls >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=register-database
LOG=${LOG_DIR}/${APP}.log
echo $(TIMESTAMP)"Register database ${DATABASE} -> ${LOG}"
../tools/${APP}.py ${DATABASE_OPT} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/${SOURCE_DATABASE}.json >& ${LOG}
if [ $? -ne 0 ] ; then
  echo $(TIMESTAMP)FAILED;
  exit 1;
fi
 
APP=register-table
for TABLE in ${ALL_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log;
  echo $(TIMESTAMP)"Register table ${TABLE} -> ${LOG}";
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/tables/${TABLE}.json >& ${LOG};
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=async-contrib-chunks
for TABLE in ${PARTITIONED_TABLES}; do
  LOG=${LOG_DIR}/${APP}-${TABLE}.log
  CSV_DIALECT="${TABLE}_CSV_DIALECT"
  echo $(TIMESTAMP)"Ingest chunk contributions into ${TABLE} -> ${LOG}"
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${!CSV_DIALECT} ${VERBOSE_OPT} ${DEBUG_OPT} ${INDIR}/data/${TABLE}.urls >& ${LOG}
  if [ $? -ne 0 ] ; then
    echo $(TIMESTAMP)FAILED;
    exit 1;
  fi;
done

APP=async-contrib-table
for TABLE in ${FULLY_REPLICATED_TABLES}; do
  URL=$(cat ${INDIR}/data/${TABLE}.urls)
  LOG=${LOG_DIR}/${APP}-${TABLE}.log
  CSV_DIALECT="${TABLE}_CSV_DIALECT"
  echo $(TIMESTAMP)"Ingest table contributions into ${TABLE} -> ${LOG}"
  ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${!CSV_DIALECT} ${VERBOSE_OPT} ${DEBUG_OPT} --url=${URL} >& ${LOG}
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

