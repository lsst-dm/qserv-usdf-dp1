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
SOURCE_DATABASE=dp02_dc2_catalogs
SOURCE_DATABASE_OPT="--database=${SOURCE_DATABASE}"
SOURCE_CONFIG_OPT="--qserv=source-qserv.json"
DATABASE=${SOURCE_DATABASE}
DATABASE_OPT="--database=${DATABASE}"
VERBOSE_OPT="--verbose"
DEBUG_OPT=
DIRECTOR_TABLES="Object Source DiaObject TruthSummary"
DEPENDENT_TABLES="ForcedSource DiaSource ForcedSourceOnDiaObject MatchesTruth"
PARTITIONED_TABLES="Object Source ForcedSource DiaObject DiaSource ForcedSourceOnDiaObject TruthSummary MatchesTruth"
FULLY_REPLICATED_TABLES="Visit CcdVisit CoaddPatches"
ALL_TABLES="${PARTITIONED_TABLES} ${FULLY_REPLICATED_TABLES}"

# CSV dialect definitions for the tables
Object_CSV_DIALECT=
Source_CSV_DIALECT=
ForcedSource_CSV_DIALECT=
DiaObject_CSV_DIALECT=
DiaSource_CSV_DIALECT='--fields-enclosed-by="'
ForcedSourceOnDiaObject_CSV_DIALECT=
Visit_CSV_DIALECT='--fields-enclosed-by="'
CcdVisit_CSV_DIALECT='--fields-enclosed-by="'
ObsCore_CSV_DIALECT='--fields-enclosed-by=",'
CoaddPatches_CSV_DIALECT='--fields-enclosed-by="'
TruthSummary_CSV_DIALECT='--fields-enclosed-by="'
MatchesTruth_CSV_DIALECT='--fields-enclosed-by="'

INDIR=${SOURCE_DATABASE}
rm -rf ${INDIR}
mkdir -p ${INDIR}/tables
mkdir -p ${INDIR}/data
mkdir -p ${INDIR}/indexes

# Prepare the confguration file qserv.json. The file will contain the authorization
# context for the subsequent operations performed by the ingest tools.
source make_config.source

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
  for idx in $(ls ../indexes-${SOURCE_DATABASE}/ | grep "_${TABLE}_" | grep json); do
    LOG=${LOG_DIR}/${APP}-${idx::-5}.log;
    echo $(TIMESTAMP)"Create table index ${idx::-5} -> ${LOG}";
    ../tools/${APP}.py ${DATABASE_OPT} --table=${TABLE} ${VERBOSE_OPT} ${DEBUG_OPT} ../indexes-${SOURCE_DATABASE}/${idx} >& ${LOG};
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

