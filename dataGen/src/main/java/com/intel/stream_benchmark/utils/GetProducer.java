package com.intel.stream_benchmark.utils;

import com.alibaba.fastjson.JSONObject;
import com.intel.stream_benchmark.ClickProducer;
import com.intel.stream_benchmark.common.ConfigLoader;
import com.intel.stream_benchmark.common.StreamBenchConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class GetProducer extends Thread{

    private String topic;
    private Long time;
    private ConfigLoader cl;
    public GetProducer(String topic, Long time , ConfigLoader cl){

        super();
        this.topic = topic;
        this.time = time;
        this.cl = cl;
    }

    @Override
    public void run() {

        System.out.println(Thread.currentThread().getName() + "=======");

        if (topic.equals("topic1")){
            datagenTopic1(cl);
        }
        else if(topic.equals("topic2")){
            datagenTopic2(cl);
        }
        else if(topic.equals("topic3")){
             new ClickProducer(time, cl).start();
        }else{
            System.out.println("No such topic!");
        }

    }

    private KafkaProducer createProducer(ConfigLoader cl) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST));
//        properties.put("batch.size", 1638400);
//        properties.put("buffer.memory", 33554432);
//        properties.put("linger.ms",50);
        return new KafkaProducer<>(properties);

    }

    private void datagenTopic1(ConfigLoader cl){

        String[] commodities = {"milk", "bag", "book","desk","sweet", "food", "disk","pen", "shoe", "animal","phone", "paper", "cup", "light", "glass", "power", "GameBoy", "chopsticks"};
        Random random = new Random();
        KafkaProducer producer = createProducer(cl);
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long start = System.currentTimeMillis();
        Boolean flag = true;
        while(flag){
            producer.send(new ProducerRecord("shopping", UUID.randomUUID().toString().replace("-", "") + "," + commodities[random.nextInt(commodities.length)] + "," +System.currentTimeMillis()));
            try{
                TimeUnit.MILLISECONDS.sleep(1);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            if((System.currentTimeMillis() - start) > time*1000){
                flag = false;
            }
        }
        producer.close();
    }

    private void datagenTopic2(ConfigLoader cl){
        Long count = 0L;
        Long totalLength = 0L;

        KafkaProducer producer = createProducer(cl);
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        Boolean flag = true;

        Random random = new Random();
        String strategy_all[] ={"t1","t2","t3","t4","t5","t6"};//t1:strategy1, t2:strategy2,,, t6:strategy6
        String site_all[] ={"1","2","3"};//1:baidu media,2:toutiao media,3: weibo media
        String pos_id_all[] ={"a","b","c"};//a:ad space,b:ad space,c:ad space
        String poi_id_all[] ={"1001","1002","1003"};//1001:ad material,1002:ad material,1003:ad material
        String cost_all[] ={"0.01","0.02","0.03"};//cost
        String device_id_all[] ={"aaaaa","bbbbb","ccccc","ddddd","eeeee","fffff","ggggg"};//device
        while(flag){

            try{

                JSONObject imp = new JSONObject();
                imp.put("imp_time",Long.valueOf(System.currentTimeMillis()));
                imp.put("strategy",strategy_all[random.nextInt(strategy_all.length-1)]);
                imp.put("site",pos_id_all[random.nextInt(site_all.length-1)]);
                imp.put("pos_id",strategy_all[random.nextInt(pos_id_all.length-1)]);
                imp.put("poi_id",poi_id_all[random.nextInt(poi_id_all.length-1)]);
                imp.put("cost",cost_all[random.nextInt(cost_all.length-1)]);
                imp.put("device_id",device_id_all[random.nextInt(device_id_all.length-1)]);
                //send exposure log
                byte[] imp_message  = imp.toJSONString().getBytes();
                producer.send(new ProducerRecord("imp",imp_message));
                count++;
                totalLength = totalLength + imp_message.length;
                //System.out.println("Exposure log:"+imp.toJSONString());
     //           TimeUnit.MILLISECONDS.sleep(1);

                if (random.nextInt(4) ==1){//the probablity of triggerring Click
                    JSONObject click =imp;
                    click.remove("imp_time");
                    click.remove("cost");
                    click.put("click_time",Long.valueOf(System.currentTimeMillis()));
                    byte[] click_message = click.toJSONString().getBytes();
                    producer.send(new ProducerRecord("click",click_message));
                    count++;
                    totalLength = totalLength + click_message.length;
                    //System.out.println("click::"+click.toJSONString());
     //               TimeUnit.MILLISECONDS.sleep(1);
                    if (random.nextInt(2) ==1){//dau time,?50
                        JSONObject dau = new JSONObject();
                        dau.put("dau_time",Long.valueOf(System.currentTimeMillis()));
                        dau.put("device_id",click.get("device_id").toString());
                        byte[] dau_message = dau.toJSONString().getBytes();
                        producer.send(new ProducerRecord("dau",dau_message));
                        count++;
                        totalLength = totalLength + dau_message.length;
                        //System.out.println("dau::"+dau.toJSONString());
                    }
                }

                if((System.currentTimeMillis() - start) > time*1000){
                    flag = false;
                    File resultFile = new File( "/home/streaming_benchmark/result/kafkaProducer.log" );
                    if (!resultFile.exists()) {
                        resultFile.createNewFile();
                    }
                    FileWriter fileWriter = new FileWriter("/home/streaming_benchmark/result/kafkaProducer.log" , true);
                    BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
                    bufferWriter.write(Thread.currentThread().getName() + "Topic2  Runtime: " + time + " Count:" + count +  "; Total data: " + totalLength + "B" +"\r\n");
                    bufferWriter.close();
                }

            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }






}
