#!/bin/bash

./configure_mailx.sh

echo "  *******************************  importing jobs table  *******************************"

export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1:$LD_LIBRARY_PATH

startDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-2hour")
endDate=$(date -u '+%Y-%m-%d %H:00:00' -d "-1hour")
echo "start date: ${startDate}"
echo "end date: ${endDate}"


python3 Jobs/job_indexer.py "${startDate}" "${endDate}" "atlas_jobs_enr" >> job_indexer.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then
    mail -v -s "ES Job Indexer: CRITICAL." $ESA_EMAIL < job_indexer.log
    echo "CRITICAL: problem with job indexer. Exiting."
    exit $rc
fi
cat job_indexer.log
# send log just for test purposes
#mail -v -s "ES Job Indexer: Info log" $ESA_EMAIL < job_indexer.log

# check number of new indices
INDEX_INFO=($(grep "final count" job_indexer.log)) && N_INDICES=${INDEX_INFO[2]}
if [[ $? != 0 ]]; then
    echo "ERROR: Could not retrieve number of added indices!"
    mail -v -s "ES Job Indexer: ERROR." $ESA_EMAIL <<< "Could not retrieve number of added indices!"
else
    if [[ $N_INDICES =~ ^[0-9]+$ ]] ; then
        if [ $N_INDICES -gt 5000 ] ; then
            echo "Passed number of added indices check."
        else
            echo "Failed number of added indices check."
            mail -v -s "ES Job Indexer: WARNING." $ESA_EMAIL <<< "Only $N_INDICES indices were added. Please check!"
        fi
    else
        echo "ERROR: N_INDICES is not a number!"
        mail -v -s "ES Job Indexer: ERROR." $ESA_EMAIL <<< "N_INDICES is not a number!"
    fi
fi

echo "Indexing DONE"
