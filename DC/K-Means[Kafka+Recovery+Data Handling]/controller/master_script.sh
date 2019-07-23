#!/bin/bash
splits=$1
file=$2
header=$3
filelength="$(wc -l $file | sed -e 's/[[:space:]]\{1,\}/ /' | cut -d' ' -f 1)"
echo 'File Length '$filelength
split_lines="$(($filelength / $splits))"
split="$(printf "%d" $split_lines)"
#perform splitting here
mkdir -p /dev/core/data
split -l $split $file /dev/core/data/data.
cd /dev/core/data
num_files="$(ls | grep 'data'| wc -l)"
#num_files="$(expr $num_files - 1)"
echo $num_files
if [ $num_files -gt $splits ]
then
    last_file="$(ls | grep 'data' | tail -n 1)"
    second_last_file="$(ls | grep 'data' | tail -n 2 | tail -n 1)"
    echo $last_file >> $second_last_file
    rm $last_file
    num_files="$(ls | grep 'data' | wc -l)"
    #num_files="$(expr $num_files - 1)"
    echo $num_files
fi
if [ $header -eq 1 ]
then
    #add headers to other split files
    first_file="$(ls | grep 'data'| head -n 1)"
    header_data="$(cat $first_file | head -n 1)"
    for file in $(ls | grep 'data' | tail -n +2)
    do
        sed -i "1i $header_data" $file
    done
fi
echo "Done"
