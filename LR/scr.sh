LEN="$(wc -l USA_Housing.csv | cut -d' ' -f 1)"
NUM=2
SPLIT="$((LEN / NUM))"
echo $SPLIT
mkdir out/
split -d -l $SPLIT USA_Housing.csv out/source.
COUNT=0
for FILE in out/*
do      
        if [ $COUNT -eq $NUM ]
        then
                break
        fi
        NAME_LEN=${#FILE}
        WORK_NUM_FULL="$(echo $FILE | sed -n "s/^.*\.\(\S*\)/\1/p")"
        WORK_NUM="$(echo $FILE | sed -n "s/^.*\.[0-9]\(\S*\)/\1/p")"
        echo 'worker'$WORK_NUM
        kubectl cp 'out/source.'$WORK_NUM_FULL 'worker'$WORK_NUM':/app/USA_Housing.csv'
done