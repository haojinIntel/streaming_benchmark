#!/bin/bash

curDir=$(cd  `dirname $0`;pwd)
#curDir=`dirname $0`
echo $curDir
rootDir=$(dirname $curDir)
echo $rootDir

if [ -e $rootDir/conf/params.conf ]; then
    source $rootDir/conf/params.conf
fi

#echo $FLINK_HOME


mainClass=com.intel.stream_benchmark.flink.Benchmark
dataGenClass=com.intel.stream_benchmark.Datagen
echo $CONF



for sql in `cat $rootDir/conf/runSqlList`;
do
    REMOTE_CMD="nohup $JAVA_HOME/bin/java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/sqlConfig $DATAGEN_TIME $TPS $sql > $rootDir/log/${sql}.log 2>&1 &"
    echo $REMOTE_CMD > $rootDir/bin/dataGen.sh
    echo "RUNING $sql"
    for host in `cat /root/hosts`; do scp -r $rootDir/bin/dataGen.sh $host:$rootDir/bin/dataGen.sh; done
    for host in `cat /root/hosts`;do ssh $host "sh $rootDir/bin/dataGen.sh"; done

#       do ssh -l \$host " java -cp \$rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  \$dataGenClass \$rootDir/conf/sqlConfig \$DATAGEN_TIME \$TPS \$sql > \$rootDir/log/${sql}.log 2>&1 &"
#        do echo $host
#        do ssh -t $host "$REMOTE_CMD"
#        do ssh -t $host ""
#    done
done
#    nohup java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/sqlConfig $DATAGEN_TIME $TPS $sql > $rootDir/log/${sql}.log 2>&1 &

#    for host in `cat /root/hosts`;do ssh $host
#    sleep 300
#    nohup $FLINK_HOME/bin/flink run -c $mainClass $rootDir/flink_benchmark/target/flink_benchmark-1.0-SNAPSHOT.jar $CONF $sql > $rootDir/log/dataGen_${sql}.log 2>&1 &
#    for host in `cat /root/hosts`; do ssh $host `nohup java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/sqlConfig $DATAGEN_TIME $TPS $sql > $rootDir/log/${sql}.log 2>&1 &`; done
#    for host in `cat /root/hosts`; do ssh $host" java -cp /home/streaming_benchmark/dataGen/target/dataGen-1.0-SNAPSHOT.jar  com.intel.stream_benchmark.Datagen /home/stream_benchmark/conf/sqlConfig 100 10000 q2.sql &"; done
#    sleep $DATAGEN_TIME
#    FLINK_ID=`"$FLINK_HOME/bin/flink" list | grep "$sql" | awk '{print $4}'; true`
#    $FLINK_HOME/bin/flink cancel  $FLINK_ID
#    echo $FLINK_ID
#done
