package com.intel.stream_benchmark;

import com.intel.stream_benchmark.common.ConfigLoader;
import com.intel.stream_benchmark.utils.GetProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Datagen {
    public static void main(String[] args) {

        System.out.println("------------------Already input args[]------------------");
        String confPath = args[0];
        ConfigLoader configLoader = new ConfigLoader(confPath);
        //the time to generate data
        Long time = Long.valueOf(args[1]);
        //The number of message per second generate
        int num = Integer.valueOf(args[2]);
        //the topic of Kafka
        String sqlName = args[3];

        String topic = configLoader.getProperty(sqlName);
        //the number of thread for datagen
        int producerNumber = num / 1000;
        ExecutorService pool = Executors.newFixedThreadPool(producerNumber);
        for(int i = 0; i < producerNumber; i++){
            pool.execute(new GetProducer(topic, time, configLoader));
        }
        System.out.println("============ StreamBench Data Generator ============");
        pool.shutdown();
        System.out.println("======== StreamBench Data Generator Finished ========");

    }
}
