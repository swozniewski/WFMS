#!/bin/bash

echo "  *******************************  importing task table  *******************************"

export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1:$LD_LIBRARY_PATH

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")

echo "start date: ${startDate}"
echo "end date: ${endDate}"


python3.6 Tasks/task_params_indexer.py "${startDate}" "${endDate}" 

rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with job indexer. Exiting."
    exit $rc
fi

echo "Indexing in UC DONE."
