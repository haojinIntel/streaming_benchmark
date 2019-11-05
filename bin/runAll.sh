#!/bin/bash

curDir=$(cd `dirname $0`;pwd)
rootDir=$(dirname $curDir)

if [ -e $rootDir/conf/env ]; then
    source $rootDir/conf/env
fi

mainClass1=com.intel.streaming_benchmark.flink.Benchmark
mainClass2=com.intel.streaming_benchmark.spark.Benchmark
dataGenClass=com.intel.streaming_benchmark.Datagen
HOSTNAME=`hostname`

echo "Run Flink benchmark!"
for sql in `cat $rootDir/conf/queriesToRun`;
do
    echo "Data generator start!"
    for host in `cat $rootDir/conf/dataGenHosts`;do ssh $host "sh $rootDir/utils/dataGenerator.sh $DATAGEN_TIME $TPS $sql flink"; done
    echo "RUNING $sql"
    nohup $FLINK_HOME/bin/flink run -c $mainClass1 $rootDir/flink/target/flink-1.0-SNAPSHOT.jar $CONF $sql >> $rootDir/flink/log/${sql}.log 2>&1 &
    sleep $DATAGEN_TIME
    FLINK_ID=`"$FLINK_HOME/bin/flink" list | grep "$sql" | awk '{print $4}'; true`
    $FLINK_HOME/bin/flink cancel  $FLINK_ID
    echo $FLINK_ID
    sleep 10
done

sleep 30

echo "Run Spark benchmark!"
for sql in `cat $rootDir/conf/queriesToRun`;
do
    echo "Data generator start!"
    for host in `cat $rootDir/conf/dataGenHosts`;do ssh $host "sh $rootDir/utils/dataGenerator.sh $DATAGEN_TIME $TPS $sql spark"; done
    echo "RUNING $sql"
    nohup $SPARK_HOME/bin/spark-submit --master spark://${HOSTNAME}:7077 --class $mainClass --deploy-mode client $rootDir/spark/target/spark-1.0-SNAPSHOT.jar $CONF $sql $DATAGEN_TIME >> $rootDir/spark/log/${sql}.log 2>&1 &
    sleep $DATAGEN_TIME
done