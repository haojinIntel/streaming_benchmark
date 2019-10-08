#!/bin/bash

curDir=$(cd  `dirname $0`;pwd)
#curDir=`dirname $0`
echo $curDir
rootDir=$(dirname $curDir)
echo $rootDir

if [ -e $rootDir/conf/params.conf ]; then
    source $rootDir/conf/params.conf
fi


mainClass=com.intel.stream_benchmark.flink.Benchmark
dataGenClass=com.intel.stream_benchmark.Datagen


for sql in `cat $rootDir/conf/runSqlList`;
do
    echo "RUNING $sql"
#    nohup java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/sqlConfig $DATAGEN_TIME $TPS $sql &
#    sleep 10
    $FLINK_HOME/bin/flink run -c $mainClass $rootDir/flink_benchmark/target/flink_benchmark-1.0-SNAPSHOT.jar $CONF $sql &
    sleep 20
#    sleep $DATAGEN_TIME
    echo "JDJDDS"
    FLINK_ID=`"$FLINK_HOME/bin/flink" list | grep "$sql" | awk '{print $4}'; true`
    $FLINK_HOME/bin/flink cancel  $FLINK_ID
    echo $FLINK_ID
done
