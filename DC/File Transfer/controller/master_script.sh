#!/bin/bash
splits=$1
file=$2
filelength="$(wc -l $file | sed -e 's/[[:space:]]\{1,\}/ /' | cut -d' ' -f 1)"
echo 'File Length '$filelength
#split_lines="$(($filelength / $splits))"
#split="$(printf "%d" $split_lines)"
#perform splitting here
mkdir -p /dev/core/data
split -n $splits $file /dev/core/data/data.
echo 'Done'
