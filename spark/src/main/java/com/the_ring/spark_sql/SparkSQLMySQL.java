package com.the_ring.spark_sql;

import com.the_ring.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Properties;

/**
 * SparkSQL 操作 MySQL 数据
 */
public class SparkSQLMySQL {

    public static void main(String[] args) {

        // 创建配置对象
        SparkConf conf = new SparkConf().setAppName("SparkSQLStudy").setMaster("local[*]");

        // 获取 SparkSession
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // 配置参数
        String pwd = System.getenv("MYSQL_PWD");
        String ip = System.getenv("REMOTE_IP");

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", pwd);

        // 读数据
        Dataset<Row> userDS = sparkSession.read()
                .jdbc("jdbc:mysql://" + ip + ":3306", "spark_study.user", properties);
        userDS.show();

        Dataset<User> newUserDataset = userDS.map((MapFunction<Row, User>) row ->
                        new User(row.getLong(0) * 2, "name: " + row.getString(1)),
                Encoders.bean(User.class));

        // 写数据
        newUserDataset.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://" + ip + ":3306")
                .option("dbtable", "spark_study.user")
                .option("user", "root")
                .option("password", pwd)
                .mode(SaveMode.Append)
                .save();

        Dataset<Row> userDS2 = sparkSession.read()
                .jdbc("jdbc:mysql://" + ip + ":3306", "spark_study.user", properties);
        userDS2.show();

        sparkSession.close();
    }
}
