#!/bin/bash

curDir=$(cd  `dirname $0`;pwd)
#curDir=`dirname $0`
echo $curDir
rootDir=$(dirname $curDir)
echo $rootDir

if [ -e $rootDir/conf/params.conf ]; then
    source $rootDir/conf/params.conf
fi


mainClass=com.intel.streaming_benchmark.flink.Benchmark
dataGenClass=com.intel.streaming_benchmark.Datagen


for sql in `cat $rootDir/conf/runningQueries`;
do
    REMOTE_CMD="nohup $JAVA_HOME/bin/java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/queryConfig $DATAGEN_TIME $sql $rootDir/conf/benchmarkConf.yaml >> $rootDir/flink/log/dataGen_${sql}.log 2>&1 &"
    echo $REMOTE_CMD > $rootDir/utils/dataGenerator.sh
    echo "RUNING $sql"
    for host in `cat $rootDir/conf/kafkaHosts`; do scp -r $rootDir/utils/dataGenerator.sh $host:$rootDir/utils/dataGenerator.sh; done
    for host in `cat $rootDir/conf/kafkaHosts`;do ssh $host "sh $rootDir/utils/dataGenerator.sh"; done
    nohup $FLINK_HOME/bin/flink run -c $mainClass $rootDir/flink/target/flink-1.0-SNAPSHOT.jar $CONF $sql >> $rootDir/flink/log/${sql}.log 2>&1 &
    sleep $DATAGEN_TIME
    FLINK_ID=`"$FLINK_HOME/bin/flink" list | grep "$sql" | awk '{print $4}'; true`
    $FLINK_HOME/bin/flink cancel  $FLINK_ID
    echo $FLINK_ID
    sleep 10
done

