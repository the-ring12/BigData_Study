package com.the_ring;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark1 {

    public static void main(String[] args) {
        // 1. 创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkCore");

        // 2. 创建 SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码

        // 4. 关闭 SparkContext
        sc.stop();
    }
}
