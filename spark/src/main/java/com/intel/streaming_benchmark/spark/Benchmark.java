package com.intel.streaming_benchmark.spark;

import com.intel.streaming_benchmark.common.*;
import com.intel.streaming_benchmark.utils.SchemaProvider;
import com.intel.streaming_benchmark.utils.SparkBenchConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.util.LongAccumulator;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.*;

public class Benchmark {
    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            BenchLogUtil.handleError("Usage: RunBench <ConfigFile> <QueryName>");

        ConfigLoader cl = new ConfigLoader(args[0]);
        String benchmarkConfDir = new File(args[0]).getParent();
        //spark config
        String sparkConf = benchmarkConfDir + "/../spark/conf/benchmarkConf.yaml";
        cl.merge(sparkConf);
        // Prepare configuration
        SparkBenchConfig conf = new SparkBenchConfig();
        conf.brokerList = cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST);
        conf.zkHost = cl.getProperty(StreamBenchConfig.ZK_HOST);
        conf.consumerGroup = cl.getProperty(StreamBenchConfig.CONSUMER_GROUP);
        conf.topic = QueryConfig.getTables(args[1]);
        conf.sqlLocation = benchmarkConfDir + "/../spark/query";
        conf.resultLocation = benchmarkConfDir + "/../spark/result";
        conf.sqlName = args[1];
        conf.runTime = Integer.valueOf(args[2]);
        runQuery(conf);
    }

    public static void runQuery(SparkBenchConfig config) throws Exception {

        //create SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName(config.sqlName)
//                .master("local[2]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String[] topics =  config.topic.split(",");
        Dataset<Row> df;
        LongAccumulator longAccumulator = jsc.sc().longAccumulator();
        Long startTime= System.currentTimeMillis();

        //generate table
        for(int i = 0; i < topics.length; i++){
            ExpressionEncoder<Row> encoder = SchemaProvider.provideSchema(topics[i]);
            if(topics[i].equals("shopping")){
                //read data from kafka and get primary data which need to be paresd to mutiple columns.
                df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", config.brokerList).option("subscribe", topics[i]).load().selectExpr("CAST(value AS STRING)").mapPartitions(new MapPartitionsFunction<Row, Row>() {
                            @Override
                            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                                List<Row> rows = new ArrayList<>();
                                while (input.hasNext()) {
                                    longAccumulator.add(1);
                                    Row next = input.next();
                                    String[] split = next.getString(0).split(",");
                                    rows.add(RowFactory.create(split[0],split[1],Timestamp.valueOf(DateUtils.parseLong2String(Long.valueOf(split[2])))));
                                }
                                return rows.iterator();
                            }
                        }, encoder).withWatermark("times", "4 seconds");
         //               .
            }else if(topics[i].equals("click")){
                df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", config.brokerList).option("subscribe", topics[i]).load().selectExpr("CAST(value AS STRING)").mapPartitions(new MapPartitionsFunction<Row, Row>() {
                            @Override
                            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                                List<Row> rows = new ArrayList<>();
                                while (input.hasNext()) {
                                    longAccumulator.add(1);
                                    JSONObject obj = JSONObject.parseObject(input.next().getString(0));
           //                         JSONObject obj = new JSONObject(input.next().getString(0));
                                    rows.add(RowFactory.create(Timestamp.valueOf(DateUtils.parseLong2String(obj.getLong("click_time"))), obj.getString("strategy"), obj.getString("site"), obj.getString("pos_id"), obj.getString("poi_id"), obj.getString("device_id")));
                                }
                                return rows.iterator();
                            }
                        }, encoder).withWatermark("click_time", "4 seconds");

            }else if(topics[i].equals("imp")){
                df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", config.brokerList).option("subscribe", topics[i]).load().selectExpr("CAST(value AS STRING)").mapPartitions(new MapPartitionsFunction<Row, Row>() {
                            @Override
                            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                                List<Row> rows = new ArrayList<>();
                                while (input.hasNext()) {
                                    longAccumulator.add(1);
                                    JSONObject obj = JSONObject.parseObject(input.next().getString(0));
           //                         JSONObject obj = new JSONObject(input.next().getString(0));
                                    rows.add(RowFactory.create(Timestamp.valueOf(DateUtils.parseLong2String(obj.getLong("imp_time"))), obj.getString("strategy"), obj.getString("site"), obj.getString("pos_id"), obj.getString("poi_id"), obj.getDouble("cost"), obj.getString("device_id")));
                                }
                                return rows.iterator();
                            }
                        }, encoder).withWatermark("imp_time", "4 seconds");
            }else if(topics[i].equals("dau")){
                df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", config.brokerList)
                        .option("subscribe", topics[i]).load().selectExpr("CAST(value AS STRING)").mapPartitions(new MapPartitionsFunction<Row, Row>() {
                            @Override
                            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                                List<Row> rows = new ArrayList<>();
                                while (input.hasNext()) {
                                    longAccumulator.add(1);
                                    JSONObject obj = JSONObject.parseObject(input.next().getString(0));
           //                         JSONObject obj = new JSONObject(input.next().getString(0));
                                    rows.add(RowFactory.create(Timestamp.valueOf(DateUtils.parseLong2String(obj.getLong("dau_time"))), obj.getString("device_id")));
                                }
                                return rows.iterator();
                            }
                        }, encoder).withWatermark("dau_time", "4 seconds");
            }else if(topics[i].equals("userVisit")){
                df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", config.brokerList).option("subscribe", topics[i]).load().selectExpr("CAST(value AS STRING)").mapPartitions(new MapPartitionsFunction<Row, Row>() {
                            @Override
                            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                                List<Row> rows = new ArrayList<>();
                                while (input.hasNext()) {
                                    longAccumulator.add(1);
                                    String[] split = input.next().getString(0).split(",");
                                    rows.add(RowFactory.create(split[0], Long.valueOf(split[1]), split[2], Long.valueOf(split[3]), Timestamp.valueOf(DateUtils.parseLong2String(Long.valueOf(split[4]))), split[5], split[6], split[7], split[8], split[9], split[10], split[11], Integer.valueOf(split[12])));
                                }
                                return rows.iterator();
                            }
                        }, encoder).withWatermark("actionTime", "4 seconds");
            }else{
                System.out.println("No such topic, please check your benchmarkConf.yaml");
                return;
            }

            df.createOrReplaceTempView(topics[i]);
        }

        //runQuery
        File file = new File(config.sqlLocation + "/" + config.sqlName);
        if (!file.exists()) {
            return;
        }
        try {
            String queryString = DateUtils.fileToString(file);
            Dataset<Row> sql = spark.sql(queryString);
            StreamingQuery start = sql.writeStream().outputMode("append").format("console").trigger(Trigger.ProcessingTime("30 seconds")).start();
            start.awaitTermination(config.runTime * 1000);
            System.out.println("2 Total number: " +  longAccumulator.value());

        } catch (Exception e) {
            e.printStackTrace();
        }
        Long finishTime= System.currentTimeMillis();
        Long runningTime = (finishTime - startTime) / 1000;
        File resultFile = new File(config.resultLocation + "/result.log" );
        if (!resultFile.exists()) {
            resultFile.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(config.resultLocation + "/result.log" , true);
        BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
        bufferWriter.write("Finished time: "+ DateUtils.parseLong2String(finishTime) + "; " + config.sqlName + "  Runtime: " + runningTime + " TPS:" + longAccumulator.value()/runningTime + "\r\n");
        bufferWriter.close();

    }
}
