# streaming_benchmark
Streaming Benchmark is designed to measure the performance of stream processing system such as flink and spark. Two use cases are simulated (User Visit Session Analysis, Evaluation of Real-time Advertising). Raw data is generated and stored in Kafka. Streams map into streaming tables and queries act on these tables.

# Building 
mvn clean package

# Prerequisites
You should have Apache Kafka, Apache zookeeper and Blink installed in your cluster.

# Setup
You need to update conf/benchmarkConf.yaml (about kafka, zookeeper, flink and queries) and runList (queries you want to run)  and params.conf(to define running time and TPS).
benchmarkConf.yaml

streambench.flink.parallelism	2
streambench.flink.bufferTimeout	10
streambench.flink.checkpointDuration	5000
streambench.zkHost	The configuration of zookeeper(ip1:port,ip2:port)
streambench.kafka.brokerList	The configuration of kafka (ip1:port1, ip1:port2.)
streambench.kafka.consumerGroup	Define the group name for consumer

runLisit
q1.sql
q2.sql
q3.sql
q4.sql
q5.sql

Params.conf
export DATAGEN_TIME=300	The running time of each queries(unit: second )
export TPS=1000	The number of message generated per second. 

# Run Benchmark
Run bin/runBenchmark.sh  and you will get the result on result/result.log.

# Result
The format of result is just like “sqlName  Runtime:<value>  TPS:<value>
result.log
q1.sql  	Runtime: 305673	TPS:891
q2.sql  	Runtime: 305920 	TPS:175
q3.sql  	Runtime: 305663 	TPS:705

