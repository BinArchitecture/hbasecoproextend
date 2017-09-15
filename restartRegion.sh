#!/bin/bash
mark=`ssh $1 -C "jps|grep HRegionServer"`
for i in {1..7}
do
if test -z "$mark"
then
sleep 3
ssh $1 -C "hbase-daemon.sh start regionserver"
exit 0
else
echo "wait $mark stop..."
sleep 1
fi
done
