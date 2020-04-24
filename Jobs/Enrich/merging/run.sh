#!/bin/bash
echo "  *******************************  Updateing merging jobs  *******************************"

export LD_LIBRARY_PATH=/usr/lib/oracle/12.2/client64/lib:$LD_LIBRARY_PATH

python3.6 update_merging.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem with updates. Exiting."
    exit $rc
fi
