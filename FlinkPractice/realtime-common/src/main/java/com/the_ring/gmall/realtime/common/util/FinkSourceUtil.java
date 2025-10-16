package com.the_ring.gmall.realtime.common.util;

import com.the_ring.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

/**
 * @Description FlinSource 工具类
 * @Date 2025/10/16
 * @Author the_ring
 */
public class FinkSourceUtil {


    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static MySqlSource<String> getMysqlSource(String database, String table) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database)
                .tableList(database + "." + table)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
    }
}
