#!/bin/bash

curDir=$(cd  `dirname $0`;pwd)
rootDir=$(dirname $curDir)

if [ -e $rootDir/conf/env ]; then
    source $rootDir/conf/env
fi

mainClass=com.intel.streaming_benchmark.spark.Benchmark
dataGenClass=com.intel.streaming_benchmark.Datagen
HOSTNAME=`hostname`

for sql in `cat $rootDir/conf/queriesToRun`;
do
    echo "Data generator start!"
    for host in `cat $rootDir/conf/dataGenHosts`;do ssh $host "sh $rootDir/utils/dataGenerator.sh $DATAGEN_TIME $THREAD_PER_NODE $sql spark"; done
    echo "RUNING $sql"
    nohup $SPARK_HOME/bin/spark-submit --master spark://${HOSTNAME}:7077 --class $mainClass --deploy-mode client $rootDir/spark/target/spark-1.0-SNAPSHOT.jar $CONF $sql $DATAGEN_TIME >> $rootDir/spark/log/${sql}.log 2>&1 &
#   $SPARK_HOME/bin/spark-submit --master spark://${HOSTNAME}:7077 --class $mainClass --deploy-mode client $rootDir/spark/target/spark-1.0-SNAPSHOT.jar $CONF $sql $DATAGEN_TIME
    sleep $DATAGEN_TIME
done
