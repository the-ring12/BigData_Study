package com.the_ring.gmall.realtime.dim.function;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.the_ring.gmall.realtime.common.bean.TableProcessDim;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Description 处理连接广播的配置流后的处理函数
 * @Date 2025/10/16
 * @Author the_ring
 */
public class ConfigBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private Map<String, TableProcessDim> configMap = new HashMap<>();
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    public ConfigBroadcastProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // 将配置表中的配置信息预加载程序中
        // 读取 MySQL 表：加载驱动->建立连接->获取数据操作对象->执行SQL->处理结果集->释放资源
        Class.forName(Constant.MYSQL_DRIVER);
        java.sql.Connection con = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        String sql = "SELECT * FROM gmall2025_config.table_process_dim";
        PreparedStatement statement = con.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        Connection connection = HBaseUtil.getHBaseConnection();
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = resultSet.getObject(i);
                jsonObject.put(columnName, columnValue);
            }
            TableProcessDim tableProcessDim = jsonObject.toJavaObject(TableProcessDim.class, JSONReader.Feature.SupportSmartMatch);
            // 并在 HBase 中创建表
            HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }

        resultSet.close();
        statement.close();
        con.close();
    }

    /**
     * 处理主流业务数据             根据维度表名到广播状态中读取配置信息，判断是否为维度
     * @param jsonObject The stream element.
     * @param readOnlyContext A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *     querying the current processing/event time and updating the broadcast state. The context
     *     is only valid during the invocation of this method, do not store it.
     * @param collector The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String table = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcessDim tableProcessDim = null;
        if ((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim = configMap.getOrDefault(table, null)) != null) {
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

            data.put("type", jsonObject.get("type"));

            collector.collect(Tuple2.of(data, tableProcessDim));
        }
    }

    /**
     * 处理广播流配置信息   将配置数据据放到广播状态中 k: 维度表名   v: 一个配置对象
     * @param value The stream element.
     * @param context A {@link Context} that allows querying the timestamp of the element, querying the
     *     current processing/event time and updating the broadcast state. The context is only valid
     *     during the invocation of this method, do not store it.
     * @param collector The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String op = value.getOp();
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor); // 获取广播状态
        String sourceTable = value.getSourceTable();
        if ("d".equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable, value);
            configMap.put(sourceTable, value);
        }
    }
}