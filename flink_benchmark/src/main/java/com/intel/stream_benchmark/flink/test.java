package com.intel.stream_benchmark.flink;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
        File directory = new File("");
        String courseFile = directory.getCanonicalPath();
       // System.out.println(courseFile);
        System.out.println(System.getProperty("user.dir") + "!");
    }

}
