package com.intel.streaming_benchmark;

import com.intel.streaming_benchmark.common.ConfigLoader;
import com.intel.streaming_benchmark.common.StreamBenchConfig;
import com.intel.streaming_benchmark.utils.GetProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Datagen {
    public static void main(String[] args) {

        System.out.println("------------------Already input args[]------------------");
        String confPath = args[0];
        ConfigLoader configLoader = new ConfigLoader(confPath);
        //the time to generate data
        Long time = Long.valueOf(args[1]);

        //the topic of Kafka
        String sqlName = args[2];

        String topic = configLoader.getProperty(sqlName);
        //the number of thread for datagen

        ConfigLoader configLoader1 = new ConfigLoader(args[3]);
        //The number of message per second generate
        int num = Integer.valueOf(configLoader1.getProperty(StreamBenchConfig.DATAGEN_THROUGHPUT));

        int producerNumber = num / 1000;
        ExecutorService pool = Executors.newFixedThreadPool(producerNumber);
        for(int i = 0; i < producerNumber; i++){
            pool.execute(new GetProducer(topic, time, configLoader1));
        }
        System.out.println("============ StreamBench Data Generator ============");
        pool.shutdown();
        System.out.println("======== StreamBench Data Generator Finished ========");

    }
}
