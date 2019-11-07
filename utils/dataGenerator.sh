#!/bin/bash

curDir=$(cd `dirname $0`;pwd)
#curDir=`dirname $0`
echo $curDir
rootDir=$(dirname $curDir)
echo $rootDir

DATAGEN_TIME=$1
THREAD_PER_NODE=$2
SQL=$3
ENGINE=$4


/opt/Beaver/jdk/bin/java -cp $rootDir/dataGen/target/dataGen-1.0-SNAPSHOT.jar com.intel.streaming_benchmark.Datagen $DATAGEN_TIME $THREAD_PER_NODE $SQL $rootDir/conf/benchmarkConf.yaml >> $rootDir/$ENGINE/log/dataGen_${SQL}.log 2>&1 &
