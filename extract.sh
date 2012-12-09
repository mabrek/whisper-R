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


WHISPER_DIR="${GRAPHITE_STORAGE_DIR}/whisper"

if [ ! -d "$WHISPER_DIR" ]
then
  echo "Fatal Error: $WHISPER_DIR does not exist."
  exit 1
fi

cd $WHISPER_DIR
WSPS=`find -L . -name '*.wsp'`
for WSP in $WSPS; 
do
    DATA=`echo $WSP | perl -pe 's!^[^/]+/(.+)\.wsp$!$1!; s!/!.!g'`;
    whisper-fetch.py --from=$1 --until=$2 $WSP > $3/$DATA
done
