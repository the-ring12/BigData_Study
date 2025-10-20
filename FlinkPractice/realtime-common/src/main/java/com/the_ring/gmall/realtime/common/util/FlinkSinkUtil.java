package com.the_ring.gmall.realtime.common.util;

import com.the_ring.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @Description FlinkSink 工具类
 * @Date 2025/10/17
 * @Author the_ring
 */
public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务 Id 的前缀
                .setTransactionalIdPrefix("dwd_base_log_" + topic)
                // 事务超时时间比检查点超时时间 大
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 100 + "")
                //需要在消费端设置隔离级别为读已提交
                .build();
    }
}
