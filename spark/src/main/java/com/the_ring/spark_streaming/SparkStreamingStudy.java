package com.the_ring.spark_streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.*;
import java.util.concurrent.TimeoutException;

public class SparkStreamingStudy {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession spark  = SparkSession.builder()
                .appName("SparkStreaming")
                .master("local[*]")
                .getOrCreate();

        // 创建监听 localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "120.76.159.200")
                .option("port", "9999")
                .load();
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) row -> Arrays.asList(row.split(" ")).iterator(),
                        Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        // 输出
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}

