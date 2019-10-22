package com.intel.stream_benchmark.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SchemaProvider {

    public static ExpressionEncoder<Row> provideSchema(String topic){
        StructType type = new StructType();
        if(topic.equals("shopping")){
            type = type.add("userID", DataTypes.StringType)
                    .add("commodity", DataTypes.StringType)
                    .add("times", DataTypes.TimestampType);;
        }else if(topic.equals("click")){
            type = type.add("click_time", DataTypes.TimestampType)
                    .add("strategy", DataTypes.StringType)
                    .add("site", DataTypes.StringType)
                    .add("pos_id", DataTypes.StringType)
                    .add("poi_id", DataTypes.StringType)
                    .add("device_id", DataTypes.StringType);
        }else if(topic.equals("imp")){
            type = type.add("imp_time", DataTypes.TimestampType)
                    .add("strategy", DataTypes.StringType)
                    .add("site", DataTypes.StringType)
                    .add("pos_id", DataTypes.StringType)
                    .add("poi_id", DataTypes.StringType)
                    .add("cost", DataTypes.DoubleType)
                    .add("device_id", DataTypes.StringType);
        }else if(topic.equals("dau")){
            type = type.add("dau_time", DataTypes.TimestampType)
                    .add("device_id", DataTypes.StringType);
        }else if(topic.equals("userVisit")){
            type = type.add("date", DataTypes.StringType)
                    .add("userId", DataTypes.LongType)
                    .add("sessionId", DataTypes.StringType)
                    .add("pageId", DataTypes.LongType)
                    .add("actionTime", DataTypes.TimestampType)
                    .add("searchKeyword", DataTypes.StringType)
                    .add("clickCategoryId", DataTypes.StringType)
                    .add("clickProductId", DataTypes.StringType)
                    .add("orderCategoryIds", DataTypes.StringType)
                    .add("orderProductIds", DataTypes.StringType)
                    .add("payCategoryIds", DataTypes.StringType)
                    .add("payProductIds", DataTypes.StringType)
                    .add("cityId", DataTypes.IntegerType);
        }else {
            System.out.println("No such table schema!!!");
            return null;
        }

        return RowEncoder.apply(type);

    }


}
