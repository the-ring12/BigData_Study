package com.the_ring.gmall.realtime.dim.app;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.the_ring.gmall.realtime.common.base.BaseApp;
import com.the_ring.gmall.realtime.common.bean.TableProcessDim;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.FlinkSourceUtil;
import com.the_ring.gmall.realtime.dim.function.ConfigBroadcastProcessFunction;
import com.the_ring.gmall.realtime.dim.function.ConfigHBaseMapFunction;
import com.the_ring.gmall.realtime.dim.function.HBaseSinkFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description DIM 层处理
 * @Date 2025/10/9
 * @Author the_ring
 */
public class DimAPP extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimAPP().start(1001, 1, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
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
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall2025_config", "table_process_dim");

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
                .map(new ConfigHBaseMapFunction()).setParallelism(1);


        // 8. 将配置流中的配置信息进行广播——broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("MapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> configBroadcastDS = tableProcessDimDS.broadcast(mapStateDescriptor);

        // 9. 将主流业务数据和广播流配置信息进行关联——connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = kafkaObjDS
                .connect(configBroadcastDS)
                // 10. 处理关联后的数据（判断是否为维度）
                .process(new ConfigBroadcastProcessFunction(mapStateDescriptor));
        dimDS.print();

        dimDS.addSink(new HBaseSinkFunction());
    }
}
