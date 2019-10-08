package com.intel.stream_benchmark.utils;

import com.alibaba.fastjson.JSONObject;
import com.intel.stream_benchmark.ClickProducer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class KafkaProducer extends Thread{

    private String topic;
    private Long time;

    public KafkaProducer(String topic, Long time){

        super();
        this.topic = topic;
        this.time = time;

    }

    @Override
    public void run() {

        if (topic.equals("topic1")){
            datagenTopic1();
        }
        else if(topic.equals("topic2")){
            datagenTopic2();
        }
        else if(topic.equals("topic3")){
             new ClickProducer(time).start();
        }else{
            System.out.println("No such topic!");
        }

    }

    private Producer createProducer() {

        Properties properties = new Properties();
        //properties.put("zookeeper.connect","192.168.32.23:2181");
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "192.168.32.23:9093,192.168.32.23:9094,192.168.32.23:9095,192.168.26.125:9093,192.168.26.125:9094,192.168.26.125:9095");
        return new Producer<>(new ProducerConfig(properties));

    }

    private void datagenTopic1(){

        String[] commodities = {"milk", "bag", "book","desk","sweet", "food", "disk","pen", "shoe", "animal","phone", "paper", "cup", "light", "glass", "power", "GameBoy", "chopsticks"};
        Random random = new Random();
        Producer producer = createProducer();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long start = System.currentTimeMillis();
        Boolean flag = true;
        while(flag){
            producer.send(new KeyedMessage<Integer,String>("shopping", UUID.randomUUID().toString().replace("-", "") + "," + commodities[random.nextInt(commodities.length)] + "," +System.currentTimeMillis()));
            try{
                TimeUnit.MILLISECONDS.sleep(1);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            if((System.currentTimeMillis() - start) > time*1000){
                flag = false;
            }
        }
    }

    private void datagenTopic2(){

        Producer producer = createProducer();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        Boolean flag = true;
        while(flag){

            try{
                Random random = new Random();
                String strategy_all[] ={"t1","t2","t3","t4","t5","t6"};//t1:strategy1, t2:strategy2,,, t6:strategy6
                String site_all[] ={"1","2","3"};//1:baidu media,2:toutiao media,3: weibo media
                String pos_id_all[] ={"a","b","c"};//a:ad space,b:ad space,c:ad space
                String poi_id_all[] ={"1001","1002","1003"};//1001:ad material,1002:ad material,1003:ad material
                String cost_all[] ={"0.01","0.02","0.03"};//cost
                String device_id_all[] ={"aaaaa","bbbbb","ccccc","ddddd","eeeee","fffff","ggggg"};//device
                JSONObject imp = new JSONObject();
                imp.put("imp_time",Long.valueOf(System.currentTimeMillis()));
                imp.put("strategy",strategy_all[random.nextInt(strategy_all.length-1)]);
                imp.put("site",pos_id_all[random.nextInt(site_all.length-1)]);
                imp.put("pos_id",strategy_all[random.nextInt(pos_id_all.length-1)]);
                imp.put("poi_id",poi_id_all[random.nextInt(poi_id_all.length-1)]);
                imp.put("cost",cost_all[random.nextInt(cost_all.length-1)]);
                imp.put("device_id",device_id_all[random.nextInt(device_id_all.length-1)]);
                //send exposure log
                producer.send(new KeyedMessage("imp",imp.toJSONString()));
                //System.out.println("Exposure log:"+imp.toJSONString());
                TimeUnit.MILLISECONDS.sleep(1);

                if (random.nextInt(4) ==1){//the probablity of triggerring Click
                    JSONObject click =imp;
                    click.remove("imp_time");
                    click.remove("cost");
                    click.put("click_time",Long.valueOf(System.currentTimeMillis()));

                    producer.send(new KeyedMessage("click",click.toJSONString()));
                    //System.out.println("click::"+click.toJSONString());
                    TimeUnit.MILLISECONDS.sleep(1);
                    if (random.nextInt(2) ==1){//dau time,?50
                        JSONObject dau = new JSONObject();
                        dau.put("dau_time",Long.valueOf(System.currentTimeMillis()));
                        dau.put("device_id",click.get("device_id").toString());
                        producer.send(new KeyedMessage("dau",dau.toJSONString()));
                        //System.out.println("dau::"+dau.toJSONString());
                    }
                }

                if((System.currentTimeMillis() - start) > time*1000){
                    flag = false;
                }

            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }






}
