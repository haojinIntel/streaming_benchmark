#!/bin/bash

curDir=$(cd  `dirname $0`;pwd)
rootDir=$(dirname $curDir)

if [ -e $rootDir/conf/params.conf ]; then
    source $rootDir/conf/params.conf
fi


mainClass=com.intel.streaming_benchmark.spark.Benchmark
dataGenClass=com.intel.streaming_benchmark.Datagen


for sql in `cat $rootDir/conf/runningQueries`;
do
    REMOTE_CMD="nohup $JAVA_HOME/bin/java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/queryConfig $DATAGEN_TIME $sql $rootDir/conf/benchmarkConf.yaml >> $rootDir/spark/log/dataGen_${sql}.log 2>&1 &"
    echo $REMOTE_CMD > $rootDir/utils/dataGenerator.sh
    echo "RUNING $sql"
    for host in `cat $rootDir/conf/kafkaHosts`; do scp -r $rootDir/utils/dataGenerator.sh $host:$rootDir/utils/dataGenerator.sh; done
    for host in `cat $rootDir/conf/kafkaHosts`;do ssh $host "sh $rootDir/utils/dataGenerator.sh"; done
    nohup $SPARK_HOME/bin/spark-submit --master spark://sr546:7077 --class $mainClass --deploy-mode client $rootDir/spark/target/spark-1.0-SNAPSHOT.jar $CONF $sql $DATAGEN_TIME >> $rootDir/spark/log/${sql}.log 2>&1 &
#   $SPARK_HOME/bin/spark-submit --master spark://sr546:7077 --class $mainClass --deploy-mode client $rootDir/spark/target/spark-1.0-SNAPSHOT.jar $CONF $sql $DATAGEN_TIME
   sleep $DATAGEN_TIME
done
