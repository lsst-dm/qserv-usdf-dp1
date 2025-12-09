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

# Variables that define a scope of the ingest
SOURCE_DATABASE=dp02_dc2_catalogs
DATABASE=dp02_dc2_catalogs_01
DATABASE_OPT="--database=${DATABASE}"
VERBOSE_OPT="--verbose"
DEBUG_OPT=
PARTITIONED_TABLES="Object Source ForcedSource DiaObject DiaSource ForcedSourceOnDiaObject TruthSummary MatchesTruth"
FULLY_REPLICATED_TABLES="Visit CcdVisit CoaddPatches"
ALL_TABLES="${PARTITIONED_TABLES} ${FULLY_REPLICATED_TABLES}"

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

echo $(TIMESTAMP)DONE

