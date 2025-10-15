package com.the_ring.gmall.realtime.dim;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.the_ring.gmall.realtime.common.bean.TableProcessDim;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Description DIM 层处理
 * @Date 2025/10/9
 * @Author the_ring
 */
public class DimAPP {
    public static void main(String[] args) throws Exception {
        // 1. 基本环境配置
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 1.2 设置并行度
        env.setParallelism(4);

        // 2. 检查点相关配置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2.2 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
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
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId("dim_app_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 4. 对业务流中的数据类型进行转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> kafkaObjDS = kafkaStrDS.process(
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
//        kafkaObjDS.print();

        // 5. 使用 Flink CDC 读取配置表中的配置信息
        // 5.1 创建 MysqlSource 对象
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall2025_config")
                .tableList("gmall2025_config.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.earliest())
                .jdbcProperties(props)
                .build();
        // 5.2 读取数据，封装为流
        DataStreamSource<String> mySQLStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1); // 配置流需设置为 1，否则流入不同分区会出现乱序
        //        mySQLStrDS.print();
        // 6. 对配置流中的数据类型进行转换 jsonStr->jsonObj
        SingleOutputStreamOperator<TableProcessDim> tableProcessDimDS = mySQLStrDS.map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim;
                        if ("d".equals(op)) {
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class, JSONReader.Feature.SupportSmartMatch);
                        } else {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class, JSONReader.Feature.SupportSmartMatch);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }).setParallelism(1)
                //        tableProcessDimDS.print();
                // 7. 根据配置表中的配置信息到 HBase 中执行建表或删表操作
                .map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection connection;


                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        connection = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        String op = tableProcessDim.getOp();
                        if ("d".equals(op)) {
                            HBaseUtil.dropHbaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                        } else if ("r".equals(op) || "c".equals(op)) {
                            HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
                        } else {
                            HBaseUtil.dropHbaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                            HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
                        }
                        return tableProcessDim;
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(connection);
                    }
                }).setParallelism(1);


        // 8. 将配置流中的配置信息进行广播——broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("MapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> configBroadcastDS = tableProcessDimDS.broadcast(mapStateDescriptor);

        // 9. 将主流业务数据和广播流配置信息进行关联——connect
        kafkaObjDS.connect(configBroadcastDS)
                // 10. 处理关联后的数据（判断是否为维度）
                // processElement: 处理主流业务数据             根据维度表名到广播状态中读取配置信息，判断是否为维度
                // processBroadcastElement: 处理广播流配置信息   将配置数据据放到广播状态中 k: 维度表名   v: 一个配置对象
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {



                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        String table = jsonObject.getString("table");
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        TableProcessDim tableProcessDim = broadcastState.get(table);
                        if (tableProcessDim != null) {
                            // 处理的是维度数据，传递数据到下流
                            JSONObject data = jsonObject.getJSONObject("data");

                            // 过滤无用数据字段
                            List<String> columnList = Arrays.asList(tableProcessDim.getSinkColumns().split(","));
                            Set<Map.Entry<String, Object>> entrySet = data.entrySet();
//                            Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
//                            while (iterator.hasNext()) {
//                                Map.Entry<String, Object> entry = iterator.next();
//                                if (!columnList.contains(entry.getKey())) {
//                                    iterator.remove();
//                                }
//                            }
                            entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));

                            collector.collect(Tuple2.of(data, tableProcessDim));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                        String op = value.getOp();
                        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor); // 获取广播状态
                        String sourceTable = value.getSourceTable();
                        if ("d".equals(op)) {
                            broadcastState.remove(sourceTable);
                        } else {
                            broadcastState.put(sourceTable, value);
                        }
                    }
                })
                .print()
        ;

        // 11. 将维度数据同步到 HBase 表中

        env.execute();
    }
}
