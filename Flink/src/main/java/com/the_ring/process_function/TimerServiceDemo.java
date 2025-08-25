package com.the_ring.process_function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Description 定时器 TimerService Study
 * @Date 2025/8/25
 * @Author the_ring
 */
public class TimerServiceDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9090);
        socketSource.print();




        env.execute();
    }
}
