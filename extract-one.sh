#!/bin/bash

WSP=$4
DATA=`echo $WSP | perl -pe 's!^[^/]+/(.+)\.wsp$!$1!; s!/!.!g'`;
# TODO realpath $3
whisper-fetch.py --from=$1 --until=$2 $WSP > $3/$DATA
