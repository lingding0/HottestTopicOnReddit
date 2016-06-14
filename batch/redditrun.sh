# check input arguments
if [ "$#" -ne 2 ]; then
    echo "Please specify key" && exit 1
fi

KEY1="$1"
KEY2="$2"

COUNTER="$KEY1"

while [ $COUNTER  -lt $KEY2 ]; do
   echo  "The counter now is" $COUNTER 
   /usr/local/spark/bin/spark-submit \
    --master spark://ip-172-31-0-104:7077 \
    sparkReddit.py $COUNTER 
   let COUNTER=COUNTER+1
   sleep 1
done
