#!/bin/bash

echo "  *******************************  importing task table  *******************************"

export LD_LIBRARY_PATH=/usr/lib/oracle/12.2/client64/lib:$LD_LIBRARY_PATH

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")

echo "start date: ${startDate}"
echo "end date: ${endDate}"


python3.6 Tasks/task_indexer.py "${startDate}" "${endDate}" 
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with job indexer. Exiting."
    exit $rc
fi

echo "Indexing in UC DONE."
