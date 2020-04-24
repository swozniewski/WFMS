#!/bin/bash

echo "  *******************************  importing task table  *******************************"

export LD_LIBRARY_PATH=/usr/lib/oracle/12.2/client64/lib:$LD_LIBRARY_PATH

startDate=$(date -u '+%Y-%m-%d %H:00:00')
echo "start date: ${startDate}"

python3.6 Tasks/task_params_indexer.py 
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with job indexer. Exiting."
    exit $rc
fi

echo "Indexing in UC DONE."
