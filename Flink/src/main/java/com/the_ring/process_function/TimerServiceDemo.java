package com.the_ring.process_function;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description 定时器 TimerService Study
 * @Date 2025/8/25
 * @Author the_ring
 */
public class TimerServiceDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 接受 socket 数据
        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9090);
//        socketSource.print();

        // 按逗号分隔生成 Tuple2
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2DS = socketSource
                .map((MapFunction<String, Tuple2<String, Integer>>) s -> {
                    String[] ss = s.split(",");
                    return new Tuple2<>(ss[0], Integer.valueOf(ss[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT));


        tuple2DS
                .assignTimestampsAndWatermarks(     // 添加 watermark
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimeStamp) -> element.f1 * 1000L)
                )
                .keyBy(element -> element.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) {
//                        String currentKey = ctx.getCurrentKey();    // key
//                        Long timestamp = ctx.timestamp();           // 事件时间
//                        System.out.println(currentKey + " -> " + timestamp);

                        // 定时器
                        TimerService timerService = ctx.timerService();
                        timerService.registerEventTimeTimer(5000L);

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.OnTimerContext ctx, Collector<String> out) {
                        System.out.println(ctx.getCurrentKey() + " -> " + timestamp);
                    }
                });


        env.execute();
    }
}
