package com.the_ring.gmall.realtime.dim;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.the_ring.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description TODO
 * @Date 2025/10/9
 * @Author the_ring
 */
public class DimAPP {
    public static void main(String[] args) throws Exception {
        // 1. 基本环境配置
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);

        // 2. 检查点相关配置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2.2 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000L);;
        // 2.3 设置 job 取消后检查点是否保留
        checkpointConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION);
        // 2.4 设置两个检查点之间的时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.failureReteRestart(3, Time.days(30), Time.seconds(3));
        // 2.6 设置状态后端以及检查点存储路径
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop00:8020/ck");
        // 2.7 设置操作 hadoop 的用户
        System.setProperty("HADOOP_USER_NAME", "the-ring");

        // 3. 从 Kafka 的 topic-db 主题中读取业务数据
        // 3.1 声明消费的主题以及消费者组
        // 3.2 创建消消费者对象
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId("dim_app_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 4. 对业务流中的数据类型进行转换 jsonStr->jsonObj
        kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {

                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        String db = jsonObject.getString("database");
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");
                        if ("gmall".equals(db)
                                && ("insert".equals(type) || "update".equals(type) || "delete".equals(type) || "bootstrap-insert".equals(type))
                                && data != null && data.length() > 2) {
                            out.collect(jsonObject);
                        }
                    }
                }
        );

        // 5. 使用 Flink CDC 读取配置表中的配置信息
        // 5.1 创建 MysqlSource 对象
        // 5.2 读取数据，封装为流

        // 6. 对配置流中的数据类型进行转换 jsonStr->jsonObj

        // 7. 根据配置表中的配置信息到 HBase 中执行建表或删表操作

        // 8. 将配置流中的配置信息进行广播——broadcast

        // 9. 将主流业务数据和广播流配置信息进行关联——connect

        // 10. 处理关联后的数据（判断是否为维度）
        // processElement: 处理主流业务数据             根据维度表名到广播状态中读取配置信息，判断是否为维度
        // processBroadcastElement: 处理广播流配置信息   将配置数据据放到广播状态中 k: 维度表名   v: 一个配置对象

        // 11. 将维度数据同步到 HBase 表中

        env.execute();
    }
}
