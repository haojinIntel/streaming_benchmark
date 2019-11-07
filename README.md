# Streaming_benchmark
Streaming Benchmark is designed to measure the performance of stream processing system such as flink and spark. Three use cases are simulated (User Visit Session Analysis, Evaluation of Real-time Advertising and Shopping Record Analysis). Raw data is generated and stored in Kafka. Streams map into streaming tables and queries act on these tables.

## Building
```
mvn clean package
```
## Prerequisites
You should have Apache Kafka, Apache zookeeper, Apache Spark and Flink-1.9 installed in your cluster.

## Setup
1. Clone the project into your master.
2. Update conf/benchmarkConf.yaml (The properties of Kafka, Zookeeper, benchmark...)
```
streambench.zkHost                       ip1:2181,ip2:2181,ip3:2181...
streambench.kafka.brokerList             ip1:port1,ip1:port2...
streambench.kafka.consumerGroup          benchmark(default)
```
3. Update flink/conf/benchmarkConf.yaml (The properties of flink)
```
streambench.flink.checkpointDuration     5000
streambench.flink.timeType               EventTime(Use EventTime or ProcessTime)
```
4. Update conf/dataGenHosts (The hosts where data will be generated; suggest to generate data on kafka node)
```
ip1
ip2
...
```
5. Update conf/queriesToRun (The queries will be run)
```
q1.sql
q2.sql
q3.sql
...
```
6. Update conf/env
```
export DATAGEN_TIME=100 (Running time for each query)
export TPS=10000(Throughput)
export FLINK_HOME={FLINK_HOME}
export SPARK_HOME={SPARK_HOME}
```
7. Copy the project to every node which will generate data (the same hosts in conf/dataGenHosts) and ensure that the master node can log in these hosts without password.

## Run Benchmark
Start Zookeeper, kafka, Spark, Flink first.
Run flink benchmark: `sh bin/runFlinkBenchmark.sh`.
Run spark benchmark: `sh bin/runSparkBenchmark.sh`.
Run both flink and spark benchmark: `sh bin/runAll.sh`.

## Result
The results will be save on flink/result/result.log and spark/result/result.log and the format of result is just like below:
```
Finished time: 2019-10-30 19:07:26; q1.sql  Runtime: 58s TPS:10709265
Finished time: 2019-10-30 19:08:37; q2.sql  Runtime: 57s TPS:8061793
Finished time: 2019-10-30 19:09:51; q5.sql  Runtime: 57s TPS:4979921
```
