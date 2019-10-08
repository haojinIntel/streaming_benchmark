package com.intel.stream_benchmark.flink;

import com.intel.stream_benchmark.common.*;
import com.intel.stream_benchmark.utils.FlinkBenchConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.csv.CsvTableSink;
import org.apache.flink.table.sinks.csv.RetractCsvTableSink;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class Benchmark {
    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            BenchLogUtil.handleError("Usage: RunBench <ConfigFile> <QueryName>");

        ConfigLoader cl = new ConfigLoader(args[0]);
        String rootDir = System.getProperty("user.dir");

        // Prepare configuration
        FlinkBenchConfig conf = new FlinkBenchConfig();
        conf.brokerList = cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST);
        conf.zkHost = cl.getProperty(StreamBenchConfig.ZK_HOST);
        conf.consumerGroup = cl.getProperty(StreamBenchConfig.CONSUMER_GROUP);
        conf.checkpointDuration = Long.parseLong(cl.getProperty(StreamBenchConfig.FLINK_CHECKPOINTDURATION));
        conf.timeType = cl.getProperty(StreamBenchConfig.FLINK_TIMETYPE);
        conf.topic = cl.getProperty(args[1]);

//        conf.sqlLocation = cl.getProperty(StreamBenchConfig.SQL_LOCATION);
//        conf.resultLocation = cl.getProperty(StreamBenchConfig.FLINK_RESULT_DIR);
        conf.sqlLocation = rootDir + "/query";
        conf.resultLocation = rootDir + "/result";
        conf.sqlName = args[1];
        runQuery(conf);
    }

    public static void runQuery(FlinkBenchConfig config) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(config.checkpointDuration);
        if(config.timeType.equals("EventTime")){
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }else{
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        }

        TableConfig tc = new TableConfig();
        StreamTableEnvironment tableEnv = new StreamTableEnvironment(env, tc);

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", config.zkHost);
        properties.setProperty("group.id", config.consumerGroup);
        properties.setProperty("bootstrap.servers", config.brokerList);
//        properties.setProperty("auto.offset.reset", config.offsetReset);

        String[] topics =  config.topic.split(",");

        //generate table
        for(int i = 0; i < topics.length; i++){
            // source stream
            FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>(topics[i], new SimpleStringSchema(),properties);
            consumer.setStartFromLatest();
            //add stream source for flink
            DataStream<String> stream = env.addSource(consumer);
            // stream parse  need table schema
            String[] fieldTypes = TpcDsSchemaProvider.getSchema(topics[i]).getFieldTypes();
            String[] fieldNames = TpcDsSchemaProvider.getSchema(topics[i]).getFieldNames();
            //  TypeInformation returnType = TypeExtractor.createTypeInfo();
            DataStream streamParsed;

            if(config.timeType.equals("EventTime")){
                if(topics[i].equals("shopping")){
                    streamParsed = stream.flatMap(new DeserializeShopping()).assignTimestampsAndWatermarks(new ShoppingWatermarks());
                }else if(topics[i].equals("click")){
                    streamParsed = stream.flatMap(new DeserializeClick()).assignTimestampsAndWatermarks(new ClickWatermarks());
                }else if(topics[i].equals("imp")){
                    streamParsed = stream.flatMap(new DeserializeImp()).assignTimestampsAndWatermarks(new ImpWatermarks());
                }else if(topics[i].equals("dau")){
                    streamParsed = stream.flatMap(new DeserializeDau()).assignTimestampsAndWatermarks(new DauWatermarks());
                }else if(topics[i].equals("userVisit")){
                    streamParsed = stream.flatMap(new DeserializeUserVisit()).assignTimestampsAndWatermarks(new UserVisitWatermarks());
                }else{
                    System.out.println("No such topic, please check your benchmarkConf.yaml");
                    return;
                }

            }else{
                if(topics[i].equals("shopping")){
                    streamParsed = stream.flatMap(new DeserializeShopping());
                }else if(topics[i].equals("click")){
                    streamParsed = stream.flatMap(new DeserializeClick());
                }else if(topics[i].equals("imp")){
                    streamParsed = stream.flatMap(new DeserializeImp());
                }else if(topics[i].equals("dau")){
                    streamParsed = stream.flatMap(new DeserializeDau());
                }else if(topics[i].equals("userVisit")){
                    streamParsed = stream.flatMap(new DeserializeUserVisit());
                }else{
                    System.out.println("No such topic, please check your benchmarkConf.yaml");
                    return;
                }
            }

            tableEnv.registerTable(topics[i], tableEnv.fromDataStream(streamParsed, FieldString(fieldNames, config.timeType)));
        }

        // define sink
        String sinkFile = "file:///home/" + config.sqlName +".csv";
        RetractCsvTableSink sink = new RetractCsvTableSink(sinkFile, ",",1, FileSystem.WriteMode.OVERWRITE);

        //runQuery
        File file = new File(config.sqlLocation + "/" + config.sqlName);
        if (!file.exists()) {
            return;
        }
        try {
            String queryString = DateUtils.fileToString(file);
            Table table = tableEnv.sqlQuery(queryString);
            table.printSchema();
            table.writeToSink(sink);
//            DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = tableEnv.toRetractStream(table, Types.TUPLE(Types.STRING, Types.LONG, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG));
//            tuple2DataStream.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

        JobExecutionResult execute = env.execute(config.sqlName);
        JobExecutionResult jobExecutionResult = execute.getJobExecutionResult();
        long netRuntime = jobExecutionResult.getNetRuntime();
        System.out.println("----------------runtime---------------- :" + netRuntime);
        long count = 0;
        for(int i = 0; i < topics.length; i++){
            Integer tmp =  (Integer)jobExecutionResult.getAccumulatorResult(topics[i]);
            count = count + tmp.longValue();
        }
        File resultFile = new File(config.resultLocation + "/result.log" );
        if (!resultFile.exists()) {
            resultFile.createNewFile();
        }
        FileWriter fileWriter = new FileWriter("result/" + resultFile.getName(), true);
        BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
        bufferWriter.write(config.sqlName + "  Runtime: " + netRuntime + " TPS:" + count/(netRuntime/1000) + "\r\n");
        bufferWriter.close();

    }

    private static String FieldString(String[] fieldNames, String timeType){
        String fileds = "";
        for(int i =0; i< fieldNames.length; i++){
            fileds = fileds + fieldNames[i] + ",";
        }
        if(timeType.equals("EventTime")){
            fileds = fileds + "rowtime.rowtime";
        }else{
            fileds = fileds + "proctime.proctime";
        }
        return fileds;
    }



    public static class ShoppingWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<String, String,Long>> {
        Long currentMaxTimestamp = 0L;
        final Long maxOutOfOrderness = 2000L;// ??????????10s
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            return watermark;
        }

        @Override
        public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
            Long timestamp = Long.valueOf(element.f2);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }


    public static class ClickWatermarks implements AssignerWithPeriodicWatermarks<Tuple6<Long,String, String,String, String, String>> {
        Long currentMaxTimestamp = 0L;
        final Long maxOutOfOrderness = 2000L;// ??????????10s
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            return watermark;
        }

        @Override
        public long extractTimestamp(Tuple6<Long, String, String, String, String, String> element, long previousElementTimestamp) {
            Long timestamp = Long.valueOf(element.f0);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }


    public static class ImpWatermarks implements AssignerWithPeriodicWatermarks<Tuple7<Long, String, String, String, String, Double, String>> {
        Long currentMaxTimestamp = 0L;
        final Long maxOutOfOrderness = 2000L;// ??????????10s
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            return watermark;
        }

        @Override
        public long extractTimestamp(Tuple7<Long, String, String, String, String, Double, String> element, long previousElementTimestamp) {
            Long timestamp = Long.valueOf(element.f0);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }


    public static class DauWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<Long,String>> {
        Long currentMaxTimestamp = 0L;
        final Long maxOutOfOrderness = 2000L;// ??????????10s
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            return watermark;
        }

        @Override
        public long extractTimestamp(Tuple2<Long, String> element, long previousElementTimestamp) {
            Long timestamp = Long.valueOf(element.f0);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }


    public static class UserVisitWatermarks implements AssignerWithPeriodicWatermarks<Tuple13<String, Long, String, Long, String, String, String, String, String, String, String, String, Integer>> {
        Long currentMaxTimestamp = 0L;
        final Long maxOutOfOrderness = 2000L;// ??????????10s
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            return watermark;
        }

        @Override
        public long extractTimestamp(Tuple13<String, Long, String, Long, String, String, String, String, String, String, String, String, Integer> element, long previousElementTimestamp) {
            Long timestamp = Long.valueOf(element.f4);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }



    public static class DeserializeShopping extends RichFlatMapFunction<String, Tuple3<String, String, Long>> {

        // Counter numLines;
        private IntCounter shopping = new IntCounter();
        @Override
        public void open(Configuration parameters) throws Exception {
            //numLines = getRuntimeContext().getMetricGroup().addGroup("flink_test_metric").counter("numLines");
            getRuntimeContext().addAccumulator("shopping", this.shopping);
            super.open(parameters);
        }

        @Override
        public void flatMap(String s, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            this.shopping.add(1);
            String[] split = s.split(",");
            collector.collect(new Tuple3<String, String, Long>(split[0], split[1], Long.valueOf(split[2])));
        }
    }

    public static class DeserializeClick extends RichFlatMapFunction<String, Tuple6<Long, String, String, String, String, String>> {

        private IntCounter click = new IntCounter();
        @Override
        public void open(Configuration parameters) throws Exception {
            //numLines = getRuntimeContext().getMetricGroup().addGroup("flink_test_metric").counter("numLines");
            getRuntimeContext().addAccumulator("click", this.click);
            super.open(parameters);
        }

        @Override
        public void flatMap(String input, Collector<Tuple6<Long, String, String, String, String, String>> collector) throws Exception {
            this.click.add(1);
            JSONObject obj = new JSONObject(input);
            Tuple6<Long, String, String, String, String, String> tuple = new Tuple6<>(
                    obj.getLong("click_time"),
                    obj.getString("strategy"),
                    obj.getString("site"),
                    obj.getString("pos_id"),
                    obj.getString("poi_id"),
                    obj.getString("device_id")
            );
            collector.collect(tuple);
        }
    }

    public static class DeserializeImp extends RichFlatMapFunction<String, Tuple7<Long, String, String, String, String, Double, String>> {

        private IntCounter imp = new IntCounter();
        @Override
        public void open(Configuration parameters) throws Exception {
            //numLines = getRuntimeContext().getMetricGroup().addGroup("flink_test_metric").counter("numLines");
            getRuntimeContext().addAccumulator("imp", this.imp);
            super.open(parameters);
        }

        @Override
        public void flatMap(String input, Collector<Tuple7<Long, String, String, String, String, Double, String>> collector) throws Exception {
            this.imp.add(1);
            JSONObject obj = new JSONObject(input);
            Tuple7<Long, String, String, String, String, Double, String> tuple = new Tuple7<>(
                    obj.getLong("imp_time"),
                    obj.getString("strategy"),
                    obj.getString("site"),
                    obj.getString("pos_id"),
                    obj.getString("poi_id"),
                    obj.getDouble("cost"),
                    obj.getString("device_id")
            );
            collector.collect(tuple);
        }
    }

    public static class DeserializeDau extends RichFlatMapFunction<String, Tuple2<Long, String>> {

        private IntCounter dau = new IntCounter();
        @Override
        public void open(Configuration parameters) throws Exception {
            //numLines = getRuntimeContext().getMetricGroup().addGroup("flink_test_metric").counter("numLines");
            getRuntimeContext().addAccumulator("dau", this.dau);
            super.open(parameters);
        }

        @Override
        public void flatMap(String input, Collector<Tuple2<Long, String>> collector) throws Exception {
            this.dau.add(1);
            JSONObject obj = new JSONObject(input);
            Tuple2<Long, String> tuple = new Tuple2<>(
                    obj.getLong("dau_time"),
                    obj.getString("device_id")
            );
            collector.collect(tuple);
        }
    }


    public static class DeserializeUserVisit extends RichFlatMapFunction<String, Tuple13<String, Long, String, Long, String, String, String, String, String, String, String, String, Integer>> {

        private IntCounter userVisit = new IntCounter();
        @Override
        public void open(Configuration parameters) throws Exception {
            //numLines = getRuntimeContext().getMetricGroup().addGroup("flink_test_metric").counter("numLines");
            getRuntimeContext().addAccumulator("userVisit", this.userVisit);
            super.open(parameters);
        }

        @Override
        public void flatMap(String s, Collector<Tuple13<String, Long, String, Long, String, String, String, String, String, String, String, String, Integer>> collector) throws Exception {
            this.userVisit.add(1);
            String[] split = s.split(",");
            Tuple13<String, Long, String, Long, String, String, String, String, String, String, String, String, Integer> tuple = new Tuple13<>(
                    split[0],
                    Long.valueOf(split[1]),
                    split[2],
                    Long.valueOf(split[3]),
                    split[4],
                    split[5],
                    split[6],
                    split[7],
                    split[8],
                    split[9],
                    split[10],
                    split[11],
                    Integer.valueOf(split[12])
            );
            collector.collect(tuple);
        }
    }


}
