package com.the_ring.gmall.realtime.common.util;

import com.alibaba.fastjson2.JSONObject;
import com.the_ring.gmall.realtime.common.bean.TableProcessDwd;
import com.the_ring.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

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

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tuple2, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                JSONObject jsonObj = tuple2.f0;
                                TableProcessDwd tableProcessDwd = tuple2.f1;
                                String topic = tableProcessDwd.getSinkTable();
                                return new ProducerRecord<byte[], byte[]>(topic, jsonObj.toJSONString().getBytes());
                            }
                        }

                )
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                // 设置事务 Id 的前缀
//                .setTransactionalIdPrefix("dwd_base_log_" + topic)
//                // 事务超时时间比检查点超时时间 大
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 100 + "")
                //需要在消费端设置隔离级别为读已提交
                .build();
    }
}
