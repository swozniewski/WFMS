#!/bin/bash

./configure_mailx.sh

echo "  *******************************  importing jobs table  *******************************"

export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1:$LD_LIBRARY_PATH

TIMESHIFT=0
if [ ! -z "$2" ]
then
  TIMESHIFT=$2
fi
NXHOURS=1
if [ ! -z "$3" ]
then
  NXHOURS=$3
fi
STARTMIN="00"
ENDMIN="00"
if (( `date +%M` >= 30 )); then
    STARTMIN="30"
    ENDMIN="30"
fi

#for archived jobs, reduce time interval by 0.5 hours
if [ "$1" = "ARCHIVED" ]; then
    if (( `date +%M` >= 30 )); then
        STARTMIN="00"
        NXHOURS=$(($NXHOURS - 1))
    else
        STARTMIN="30"
    fi
fi

startDate=$(date -u "+%Y-%m-%d %H:${STARTMIN}:00" -d "-$(($NXHOURS + $TIMESHIFT))hour")
endDate=$(date -u "+%Y-%m-%d %H:${ENDMIN}:00" -d "-${TIMESHIFT}hour")
echo "start date: ${startDate}"
echo "end date: ${endDate}"


python3 Jobs/job_indexer.py "${startDate}" "${endDate}" "atlas_jobs_enr" $1 >> job_indexer.log 2>&1
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
