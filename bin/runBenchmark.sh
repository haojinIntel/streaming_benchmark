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
    nohup $FLINK_HOME/bin/flink run -c $mainClass $rootDir/flink_benchmark/target/flink_benchmark-1.0-SNAPSHOT.jar $CONF $sql > $rootDir/log/dataGen_${sql}.log 2>&1 &
    sleep 10
    nohup java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/sqlConfig $DATAGEN_TIME $TPS $sql > $rootDir/log/${sql}.log 2>&1 &
    sleep $DATAGEN_TIME
    FLINK_ID=`"$FLINK_HOME/bin/flink" list | grep "$sql" | awk '{print $4}'; true`
    $FLINK_HOME/bin/flink cancel  $FLINK_ID
    echo $FLINK_ID
done

