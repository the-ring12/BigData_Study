package com.the_ring.gmall.realtime.common.util;

import com.the_ring.gmall.realtime.common.constant.Constant;

/**
 * @Description Flink SQL 相关工具类
 * @Date 2025/10/21
 * @Author the_ring
 */
public class SQLUtil {

    public static String getKafkaDDLSource(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +    // 当 json 解析失败时忽略这条数据
                "  'format' = 'json'\n" +
                ")";
    }


    public static String getKafkaDDLSink(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }




}
