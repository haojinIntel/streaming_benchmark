package com.intel.streaming_benchmark.flink;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class test {
    public static void main(String[] args) throws Exception{
//        File resultFile = new File("/root/stream_benchmark/result/1.log" );
//        if (!resultFile.exists()) {
//            try {
//                resultFile.createNewFile();
//            }catch (IOException e){
//                e.printStackTrace();
//            }
//        }
//        System.out.println(resultFile.getName());
//        FileWriter fileWriter = new FileWriter("result/" +resultFile.getName(), true);
//        BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
////        bufferWriter.write("\r\n");
//        bufferWriter.write("q1.sql" + "  Runtime: " + 104325 + " TPS:" + 1231423424/(104325/1000) + "\r\n" );
//        bufferWriter.close();
//        File directory = new File("");
//        String courseFile = directory.getCanonicalPath();
        String benchmarkConf = "/home/streaming_benchmark/conf/benchmarkConf.yaml";
        String filePath = new File(benchmarkConf).getParent();
        System.out.println(filePath);
//        System.out.println(System.getProperty("user.dir") + "!");


        FileWriter fileWriter = new FileWriter("/home/streaming_benchmark/conf/../flink/result/result.log" , true);
        BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
        bufferWriter.write( "  Runtime: "   + " TPS:" + "\r\n");
        bufferWriter.close();
    }

}
