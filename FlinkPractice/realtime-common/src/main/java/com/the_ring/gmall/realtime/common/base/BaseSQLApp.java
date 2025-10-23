package com.the_ring.gmall.realtime.common.base;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description Flink SQL 基类
 * @Date 2025/10/21
 * @Author the_ring
 */
public abstract class BaseSQLApp {

    public void start(int port, int parallelism, String ck) {
        // 1. 基本环境配置
        // 1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // 1.2 设置并行度
        env.setParallelism(parallelism);

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
//        checkpointConfig.setCheckpointStorage("hdfs://hadoop00:9000/ck/sql/" + ck);
        //        env.setStateBackend(new HashMapStateBackend());
        //        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop00:8020/ck/ckAndGroupId");
        // 2.7 设置操作 hadoop 的用户
        System.setProperty("HADOOP_USER_NAME", "the-ring");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. 处理逻辑
        handle(env, tableEnv);
    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);
}
