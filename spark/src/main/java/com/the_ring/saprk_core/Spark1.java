package com.the_ring.saprk_core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Spark1 {

    public static void main(String[] args) throws ClassNotFoundException {
        // 1. 创建配置对象
//        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkCore");

        // kryo 序列化
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkCore")
                // 替换默认序列化机制
                .set("apark.serializer", "org.apache.spark.serializer.KryoSerializer")
                // 注册需要使用 kryo 序列化的自定义类
                .registerKryoClasses(new Class[]{Class.forName("com.the_ring.bean.User")});

        // 2. 创建 SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        // 从内存中读取数据
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        dataRDD.collect().forEach(System.out::println);

        // 从文件读取数据
        JavaRDD<String> fileDataRDD = sc.textFile("spark/src/main/resources/data.txt");

        fileDataRDD.collect().forEach(System.out::println);

        // map() 映射
        JavaRDD<Integer> map1RDD = dataRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) throws Exception {
                return num * 2;
            }
        });

        JavaRDD<Integer> map2RDD = dataRDD.map(num -> num * 2);
        map1RDD.collect().forEach(System.out::println);

        // flatmap() 扁平化映射
        JavaRDD<String> wordRDD = fileDataRDD.flatMap(line -> Arrays.stream(line.split(" ")).iterator());

        wordRDD.collect().forEach(System.out::println);

        // groupBy()
        JavaPairRDD<Integer, Iterable<Integer>> groupRDD = dataRDD.groupBy(num -> num % 2);

        groupRDD.collect().forEach(System.out::println);

        // filter()
        JavaRDD<Integer> filterRDD = dataRDD.filter(num -> num % 2 == 1);

        filterRDD.collect().forEach(System.out::println);

        // distinct()
        JavaRDD<String> distinctRDD = wordRDD.distinct();

        distinctRDD.collect().forEach(System.out::println);

        // sortBy()
        JavaRDD<Integer> numSortedRDD = dataRDD.sortBy(num -> -num, true, 1);

        numSortedRDD.collect().forEach(System.out::println);

        JavaRDD<String> stringSortRDD = wordRDD.sortBy(s -> s.substring(s.length() - 1), true, 1);

        stringSortRDD.collect().forEach(System.out::println);

        // mapToPair()
        JavaPairRDD<Integer, Integer> pairRDD = dataRDD.mapToPair(num -> new Tuple2<>(num, num));

        pairRDD.collect().forEach(System.out::println);

        // mapValues()
        JavaPairRDD<Integer, Integer> pairMapVRDD = pairRDD.mapValues(num -> num * 2);

        pairMapVRDD.collect().forEach(System.out::println);

        // groupByKey()
        JavaPairRDD<Integer, Integer> pairRDD1 = dataRDD.mapToPair(num -> new Tuple2<>(num % 2, num));
        JavaPairRDD<Integer, Iterable<Integer>> groupByKeyRDD = pairRDD1.groupByKey();

        groupByKeyRDD.collect().forEach(System.out::println);

        // reduceByKey
        JavaPairRDD<Integer, Integer> sumRDD = pairRDD1.reduceByKey(Integer::sum);

        sumRDD.collect().forEach(System.out::println);

        // sortByKey()
        JavaPairRDD<Integer, Integer> sortByKeyRDD = pairRDD1.sortByKey();

        sortByKeyRDD.collect().forEach(System.out::println);


        // collect()
        List<String> strings = wordRDD.collect();

        // count()
        long count = wordRDD.count();
        System.out.println(count);

        // first()
        String first = wordRDD.first();
        System.out.println(first);

        // take()
        List<String> take = wordRDD.take(3);
        take.forEach(System.out::println);

        // countByKey()
        Map<Integer, Long> countByKey = pairRDD1.countByKey();
        countByKey.forEach((k, v) -> System.out.println(k + " : " + v));

        // save()
//        wordRDD.saveAsTextFile("spark/out/out1");
//
//        wordRDD.saveAsObjectFile("spark/out/out2");

        // foreach
        wordRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        // foreachPartition
        wordRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> iterator) throws Exception {
                StringBuilder stringBuilder = new StringBuilder();
                while (iterator.hasNext()) {
                    stringBuilder.append(iterator.next());
                }
                System.out.println(stringBuilder.toString());
            }
        });


        // 4. 关闭 SparkContext
        sc.stop();
    }
}
