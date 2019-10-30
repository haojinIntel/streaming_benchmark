package com.intel.streaming_benchmark.utils;

public class SparkBenchConfig {
    // Kafka related
    public String zkHost;
    public String brokerList;
    public String topic;
    public String consumerGroup;
    public String valueDeserializer;
    public String keyDeserializer;


//  public String offsetReset;
//  public String reportTopic;

    // Spark related
    public long checkpointDuration;
    public String resultLocation;
    public String sqlLocation;
    public String sqlName;
    public String timeType;


    public int runTime;

}
