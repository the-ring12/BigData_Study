package com.the_ring.spark_sql;

import com.the_ring.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * SparkSQL 对文件的操作
 */
public class SparkFile {

    public static void main(String[] args) {
        // 创建配置对象
        SparkConf conf = new SparkConf().setAppName("SparkSQLStudy").setMaster("local[*]");

        // 获取 SparkSession
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // 读取 csv
        DataFrameReader reader = sparkSession.read();
        Dataset<Row> userDS = reader
                .option("header", "true") // 读取列名，默认 false
                .option("sep", ",") // 列分隔符
                .csv("spark/src/main/resources/user.csv");
        userDS.show();

        // csv 读取的数据类型都是 string, 属性存在非 string 无法使用此方法
//        Dataset<User> userDataset = userDS.as(Encoders.bean(User.class));
//        userDataset.show();

        Dataset<User> userDataset = userDS.map((MapFunction<Row, User>) row -> new User(Long.parseLong(row.getString(1)), row.getString(0)), Encoders.bean(User.class));
        userDataset.show();

        try {
            // 写出 csv
            DataFrameWriter<User> writer = userDataset.write();
            writer.option("seq", ",")
                    .option("header", "true")
                    .option("compression", "gzip")
                    .mode(SaveMode.Append)
                    .csv("spark/out");
        } catch (Exception e) {
            e.printStackTrace();
        }


        // 读取 json
        Dataset<Row> userJson = sparkSession.read().json("spark/src/main/resources/user.json");
        Dataset<User> userDataset1 = userJson.as(Encoders.bean(User.class));

        // 写出 json
        try {
            DataFrameWriter<User> writer = userDataset1.write();
            writer.json("spark/out/UserJson");
        } catch (Exception e) {
            e.printStackTrace();
        }


        // 写出为 Parquet
        userDataset1.write().parquet("spark/out/UserParquet");

        // 读物 Parquet
        Dataset<Row> userParquet = sparkSession.read().parquet("spark/out/UserParquet");
        userParquet.printSchema();

        sparkSession.close();
    }
}
