package com.the_ring.gmall.realtime.common.util;

import com.alibaba.fastjson2.JSONObject;
import com.the_ring.gmall.realtime.common.bean.TableProcessDwd;
import com.the_ring.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

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

    /**
     * 获取 DorisSink
     * @param labelPrefix label 前缀
     * @param tableIdentify 数据库.表
     * @return DorisSink
     */
    public static DorisSink<String> getDorisSink(String labelPrefix, String tableIdentify) {

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(Constant.DORIS_FE_NODES)
                .setTableIdentifier(tableIdentify)
                .setUsername("root")
                .setPassword("")
                .build();

        Properties properties = new Properties();
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");

        DorisExecutionOptions dorisExecutionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)    // stream-load 导入数据时 label 的前缀
                .disable2PC()                   // 测试阶段禁用
                .setBufferCount(3)
                .setBufferSize(1024 * 1024)
                .setCheckInterval(3000)
                .setMaxRetries(3)
                .setDeletable(false)
                .setStreamLoadProp(properties)
                .build();


        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(dorisOptions)
                .setDorisExecutionOptions(dorisExecutionOptions)
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
