package com.the_ring.process_function;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description 实现提示每个窗口的 TopN
 * @Date 2025/8/26
 * @Author the_ring
 */
public class TopNDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 接受 socket 数据
        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9090);
//        socketSource.print();

        // 按逗号分隔生成 Tuple2
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> tuple3DS = socketSource
                .map((MapFunction<String, Tuple3<String, Integer, Integer>>) s -> {
                    String[] ss = s.split(",");
                    return new Tuple3<>(ss[0], Integer.valueOf(ss[1]), Integer.valueOf(ss[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));


        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowCountDS = tuple3DS
                .assignTimestampsAndWatermarks(     // 添加 watermark
                        WatermarkStrategy
                                .<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimeStamp) -> element.f1 * 1000L)
                )
                .keyBy(element -> element.f2)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .aggregate(new AggregateFunction<Tuple3<String, Integer, Integer>, Integer, Integer>() {
                               @Override
                               public Integer createAccumulator() {
                                   return 0;
                               }

                               @Override
                               public Integer add(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, Integer integer) {
                                   return integer + 1;
                               }

                               @Override
                               public Integer getResult(Integer integer) {
                                   return integer;
                               }

                               @Override
                               public Integer merge(Integer integer, Integer acc1) {
                                   return integer + acc1;
                               }
                           },
                        new ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) {
                                Integer val = elements.iterator().next();
                                out.collect(new Tuple3<>(key, val, context.window().getEnd()));

                            }
                        });

        windowCountDS.keyBy(element -> element.f2)
                .process(new KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>() {
                    Map<Long, List<Tuple3<Integer, Integer, Long>>> map;

                    @Override
                    public void open(OpenContext openContext) {
                        map = new HashMap<>();
                    }

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) {
                        Long key = ctx.getCurrentKey();
                        List<Tuple3<Integer, Integer, Long>> list = map.getOrDefault(key, new ArrayList<>());
                        list.add(value);
                        map.put(key, list);

                        ctx.timerService().registerEventTimeTimer(key + 1L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) {
                        Long key = ctx.getCurrentKey();
                        List<Tuple3<Integer, Integer, Long>> list = map.get(key);
                        list.sort((a, b) -> b.f1 - a.f1);

                        StringBuilder s = new StringBuilder();
                        s.append("====================================\n");
                        for (int i = 0; i < Math.min(2, list.size()); i++) {
                            s.append("Top ").append(i + 1).append("\n");
                            s.append("vc = ").append(list.get(i).f0).append("\n");
                            s.append("count = ").append(list.get(i).f1).append("\n");
                            s.append("windowEnd = ").append(key).append("\n");
                            s.append("====================================\n");
                        }

                        list.clear();
                        map.remove(key);

                        out.collect(String.valueOf(s));
                    }
                }).print();


        env.execute();
    }
}
