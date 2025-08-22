package com.the_ring.spark_sql;

import com.the_ring.bean.User;
import com.the_ring.udaf.Avg;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.Function2;
import scala.Tuple2;

/**
 * SparkSQL 基础
 */
import static org.apache.spark.sql.functions.udaf;
import static org.apache.spark.sql.functions.udf;

public class SparkSQLStudy {

    public static void main(String[] args) {
        // 创建配置对象
        SparkConf conf = new SparkConf().setAppName("SparkSQLStudy").setMaster("local[*]");

        // 获取 SparkSession
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // 按行读取
        Dataset<Row> lineDS = sparkSession.read().json("spark/src/main/resources/user.json");
//        lineDS.show();
        // 转换为类和对象
        Dataset<User> userDS = lineDS.as(Encoders.bean(User.class));
        userDS.show();
        // 函数式方法
        Dataset<User> UserDataset = lineDS.map((MapFunction<Row, User>) row -> new User(row.getLong(0), row.getString(1)), Encoders.bean(User.class));


        // SQL 方式
        lineDS.createOrReplaceTempView("user");
        Dataset<Row> result = sparkSession.sql("select * from user where age > 20");
        result.show();

        // 自定义函数
        // udf
        UserDefinedFunction prefixName = udf(new UDF1<String, String>() {
            public String call(String s) {
                return "name: " + s;
            }
        }, DataTypes.StringType);
        sparkSession.udf().register("prefixName", prefixName);
        sparkSession.sql("select prefixName(name) from user").show();

        // lambda 表达式写法
        sparkSession.udf().register("prefixName1",
                (UDF1<String, String>) name -> "addName: " + name,
                DataTypes.StringType);


        // udaf
        sparkSession.udf().register("avgAge", udaf(new Avg(), Encoders.LONG()));
        sparkSession.sql("select avgAge(age) avgAge from user").show();



        // 关闭
        sparkSession.close();
    }
}
