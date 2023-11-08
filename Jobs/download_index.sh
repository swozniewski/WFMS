source $1
WFMSWORKDIR=`dirname $BASH_SOURCE`

python3 $WFMSWORKDIR/download_index.py $2
