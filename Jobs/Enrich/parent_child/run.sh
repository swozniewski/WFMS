#!/bin/bash

# this script is called once a day
echo "  *******************************  importing parent/child table  *******************************"


export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1:$LD_LIBRARY_PATH


startDate=$(date -u '+%Y-%m-%d' -d "-48hour")
endDate=$(date -u '+%Y-%m-%d' -d "-24hour")

echo "start date: ${startDate}"
echo "end date: ${endDate}"

python3.6 parent_child_indexer.py "${startDate}" "${endDate}" 
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with job indexer. Exiting."
    exit $rc
fi

hdfs dfs -rm -R -f -skipTrash /atlas/analytics/job_parents_temp

# ./SqoopToAnalytix.sh "${startDate}" "${endDate}"

# rc=$?; if [[ $rc != 0 ]]; then 
#     echo "problem with sqoop. Exiting."
#     exit $rc; 
# fi

# echo "Sqooping DONE."

# echo "Merge data into file in temp. Will index it from there."
# rm -f /tmp/job_parents_temp.csv
# hdfs dfs -getmerge /atlas/analytics/job_parents_temp /tmp/job_parents_temp.csv

# rc=$?; if [[ $rc != 0 ]]; then 
#     echo "problem with getmerge. Exiting."
#     exit $rc; 
# fi

# echo "Running updater"
# python3.6 updater.py

# rc=$?; if [[ $rc != 0 ]]; then 
#     echo "problem with updater. Exiting."
#     exit $rc; 
# fi

echo "DONE."