#!/bin/bash

set -e

if [ "$GRAPHITE_ROOT" = "" ]
then
  GRAPHITE_ROOT="/opt/graphite"
fi

if [ "$GRAPHITE_STORAGE_DIR" = "" ]
then
  GRAPHITE_STORAGE_DIR="${GRAPHITE_ROOT}/storage"
fi

# TODO needs to be a script argument
if [ "$WHISPER_DIR" = "" ]
then
  WHISPER_DIR="${GRAPHITE_STORAGE_DIR}/whisper"
fi

if [ ! -d "$WHISPER_DIR" ]
then
  echo "Fatal Error: $WHISPER_DIR does not exist."
  exit 1
fi

export PYTHONPATH=/data/work/sources/whisper

cd $WHISPER_DIR
WSPS=`find -L . -name '*.wsp'`
for WSP in $WSPS; 
do
    DATA=`echo $WSP | perl -pe 's!^[^/]+/(.+)\.wsp$!$1!; s!/!.!g'`;
    # TODO realpath $3
    /data/work/sources/whisper/bin/whisper-fetch.py --from=$1 --until=$2 $WSP > $3/$DATA
done
