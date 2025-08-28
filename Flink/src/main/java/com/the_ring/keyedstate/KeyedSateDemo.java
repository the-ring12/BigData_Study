package com.the_ring.keyedstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description 按键分区状态
 * @Date 2025/8/27
 * @Author the_ring
 */
public class KeyedSateDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration(

        ));
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE); // 周期为 1s，精准一次
        // 获取配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000);   // 超时时间
        checkpointConfig.setMaxConcurrentCheckpoints(2);    // 最大数量
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);   // 最小等待时间
        // 取消作业是否保存到外部系统
        checkpointConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);   // 允许连续失败的次数

        // 开启非对齐
        checkpointConfig.enableUnalignedCheckpoints();
        

        // 接受 socket 数据
        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9090);

        // 按逗号分隔生成 Tuple2
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> tuple3DS = socketSource
                .map((MapFunction<String, Tuple3<String, Integer, Integer>>) s -> {
                    String[] ss = s.split(",");
                    return new Tuple3<>(ss[0], Integer.valueOf(ss[1]), Integer.valueOf(ss[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));


        tuple3DS.keyBy(val -> val.f0)
                .process(new ProcessFunction<Tuple3<String, Integer, Integer>, String>() {

                    // 定义值状态
                    ValueState<Integer> lastVal;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        super.open(openContext);

                        // 初始化值状态
                        // TTL 配置对象
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Duration.ofSeconds(10))     // 状态生存时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 更新失效时间的事件
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 失效后的可见性
                                .build();

                        // 状态描述器
                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("lastVal", Types.INT);
                        valueStateDescriptor.enableTimeToLive(ttlConfig);   // 启动 TTL

                        lastVal = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(Tuple3<String, Integer, Integer> value, ProcessFunction<Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取值
                        Integer val = lastVal.value();

                        if (val != null && Math.abs(val - value.f2) > 10) {
                            out.collect("警报：id " + value.f0 + "与上一条差值大于 10");
                        }

                        // 更新值
                        lastVal.update(value.f2);
                    }
                }).print();


        env.execute();
    }
}
