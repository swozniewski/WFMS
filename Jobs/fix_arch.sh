#!/bin/bash
# accepts only one parameter: date that has to be fixed. 
# eg. fix.sh 2018-01-01
echo "  *******************************  fixing jobs   *******************************"

export LD_LIBRARY_PATH=/usr/lib/oracle/12.2/client64/lib:$LD_LIBRARY_PATH

startDate="$1 00:00:00"
endDate="$1 23:59:59"

echo "start date: ${startDate}"
echo "end date: ${endDate}"

# JOB_ORACLE_ADG_CONNECTION_STRING

python3.6 job_indexer_arch.py "${startDate}" "${endDate}" 
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with job indexer. Exiting."
    exit $rc
fi

echo "Indexing UC DONE."