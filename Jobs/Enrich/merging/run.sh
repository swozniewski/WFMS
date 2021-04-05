#!/bin/bash
echo "  *******************************  Updateing merging jobs  *******************************"

export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1:$LD_LIBRARY_PATH

python3.6 update_merging.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with updates. Exiting."
    exit $rc
fi
