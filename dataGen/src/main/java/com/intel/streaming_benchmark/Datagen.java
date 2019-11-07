package com.intel.streaming_benchmark;

import com.intel.streaming_benchmark.common.ConfigLoader;
import com.intel.streaming_benchmark.common.QueryConfig;
import com.intel.streaming_benchmark.utils.GetProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Datagen {
    public static void main(String[] args) {

        System.out.println("------------------Already input args[]------------------");
        //the time to generate data
        Long time = Long.valueOf(args[0]);
        System.out.println("------------------time: " + time + "s-------------------");
        //the topic of Kafka
        String sqlName = args[2];
        System.out.println("------------------sql: " + sqlName + "------------------");
        String scene = QueryConfig.getScene(sqlName);

        ConfigLoader configLoader = new ConfigLoader(args[3]);
        System.out.println("------------------config: " + args[3] + "---------------");
        //the number of thread for datagen
        int producerNumber = Integer.valueOf(args[1]);
        System.out.println("----------Thread_per_node:" + producerNumber + "--------");
        ExecutorService pool = Executors.newFixedThreadPool(producerNumber);
        for(int i = 0; i < producerNumber; i++){
            pool.execute(new GetProducer(scene, time, configLoader));
        }
        System.out.println("============ StreamBench Data Generator ============");
        pool.shutdown();
        System.out.println("======== StreamBench Data Generator Finished ========");

    }
}
