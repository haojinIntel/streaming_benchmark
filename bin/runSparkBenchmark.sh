#!/bin/bash

curDir=$(cd  `dirname $0`;pwd)
rootDir=$(dirname $curDir)

if [ -e $rootDir/conf/params.conf ]; then
    source $rootDir/conf/params.conf
fi


mainClass=com.intel.stream_benchmark.spark.Benchmark
dataGenClass=com.intel.stream_benchmark.Datagen


for sql in `cat $rootDir/conf/runSqlList`;
do
    REMOTE_CMD="nohup $JAVA_HOME/bin/java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/sqlConfig $DATAGEN_TIME $TPS $sql > $rootDir/log/${sql}.log 2>&1 &"
    echo $REMOTE_CMD > $rootDir/bin/dataGen.sh
    echo "RUNING $sql"
    for host in `cat $rootDir/conf/hosts`; do scp -r $rootDir/bin/dataGen.sh $host:$rootDir/bin/dataGen.sh; done
    for host in `cat $rootDir/conf/hosts`;do ssh $host "sh $rootDir/bin/dataGen.sh"; done
#   nohup java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar  $dataGenClass $rootDir/conf/sqlConfig $DATAGEN_TIME $TPS $sql > $rootDir/log/${sql}.log 2>&1 &
    nohup $SPARK_HOME/bin/spark-submit --master spark://bdpe43:7077 --class $mainClass --deploy-mode client $rootDir/spark_benchmark/target/spark_benchmark-1.0-SNAPSHOT.jar $CONF $sql $DATAGEN_TIME > $rootDir/log/dataGen_${sql}.log 2>&1 &
    sleep $DATAGEN_TIME
done
