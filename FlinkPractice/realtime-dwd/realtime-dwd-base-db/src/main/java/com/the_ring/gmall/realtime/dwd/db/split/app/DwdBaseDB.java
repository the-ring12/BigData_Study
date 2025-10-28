package com.the_ring.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.the_ring.gmall.realtime.common.base.BaseApp;
import com.the_ring.gmall.realtime.common.bean.TableProcessDwd;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.FlinkSinkUtil;
import com.the_ring.gmall.realtime.common.util.FlinkSourceUtil;
import com.the_ring.gmall.realtime.dwd.db.split.function.BaseDBTableProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description 动态分流
 * @Date 2025/10/28
 * @Author the_ring
 */
public class DwdBaseDB extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwdBaseDB().start(10019, 4, "dwd_base_db", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        // 1. 对类型进行转换并进行简单 etl
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String type = jsonObj.getString("type");
                    if (!type.startsWith("bootstrap-")) {
                        out.collect(jsonObj);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("不是一个标准 JSON!!");
                }
            }
        });
//        jsonObjDS.print();

        // 2. Flink CDC 读取配置表数据
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("gmall2025_config", "table_process_dwd");
        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDwdDS = mysqlDS.map(new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDwd tableProcessDwd = null;
                if ("d".equals(op)) {
                    tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class, JSONReader.Feature.SupportSmartMatch);
                } else {
                    tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class, JSONReader.Feature.SupportSmartMatch);
                }
                tableProcessDwd.setOp(op);
                return tableProcessDwd;
            }
        });
        tableProcessDwdDS.print();

        // 3. 对配置流进行广播
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tableProcessDwdDS.broadcast(mapStateDescriptor);
        // 4. 关联配置流并处理
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDBTableProcessFunction(mapStateDescriptor));
//        splitDS.print();
        // 5. 写出到 Kafka
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = FlinkSinkUtil.getKafkaSink();
        splitDS.sinkTo(kafkaSink);
    }
}
