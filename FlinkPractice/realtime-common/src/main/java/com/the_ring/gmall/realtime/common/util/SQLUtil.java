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

    public static String getHbaseDDL(String namespace, String table) {
        return "WITH (\n" +
                " 'connector' = 'hbase-2.6',\n" +
                " 'table-name' = '" + namespace + ":" + table + "',\n" +
                " 'zookeeper.quorum' = 'hadoop00:2181,hadoop01:2181',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.partial-cache.max-rows' = '20',\n" +
                " 'lookup.partial-cache.expire-after-access' = '2 hour'\n" +
                " )";
    }


}
