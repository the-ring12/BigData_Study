package com.the_ring.flinkcdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description Flink CDC 基于 MySQL
 * @Date 2025/8/29
 * @Author the_ring
 */
public class FlinkDCDDemo {

    public static void main(String[] args) throws Exception {

        // 1. 获取 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 开启 Checkpoint


        // 3. 使用 FlinkCDC 构造 MySQLSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("120.79.166.200")
                .port(3306)
                .username("root")
                .password("xxxxxxxxx")
                .databaseList("test")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // 4. 读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        // 5. 打印
        mysqlDS.print();

        // 6. 启动执行
        env.execute();
    }
}
