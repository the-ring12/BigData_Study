package com.the_ring.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson2.JSONObject;
import com.the_ring.gmall.realtime.common.bean.TableProcessDwd;
import com.the_ring.gmall.realtime.common.util.JDBCUtil;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * @Description 广播后的处理
 * @Date 2025/10/28
 * @Author the_ring
 */
public class BaseDBTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {

    MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    Map<String, TableProcessDwd> configMap;

    public BaseDBTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        configMap = new HashMap<>();
        Connection mysqlConnection = JDBCUtil.getMysqlConnection();
        List<TableProcessDwd> lists = JDBCUtil.queryList(mysqlConnection, "SELECT * FROM gmall2025_config.table_process_dwd", TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : lists) {
            String key = getKey(tableProcessDwd);
            configMap.put(key, tableProcessDwd);
        }
        JDBCUtil.closeMysqlConnection(mysqlConnection);
    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        String key = getKey(table, type);
        TableProcessDwd tableProcessDwd;
        if (((tableProcessDwd = broadcastState.get(key)) != null) || ((tableProcessDwd = configMap.get(key)) != null)) {
            JSONObject data = jsonObj.getJSONObject("data");
            deleteNotNeedColumns(data, tableProcessDwd.getSinkColumns());
            data.put("ts", jsonObj.getLong("ts"));
            out.collect(Tuple2.of(data, tableProcessDwd));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        String op = tableProcessDwd.getOp();
        String key = getKey(tableProcessDwd);
        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if ("d".equals(op)) {
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            broadcastState.put(key, tableProcessDwd);
            configMap.put(key, tableProcessDwd);
        }
    }

    private String getKey(TableProcessDwd tableProcessDwd) {
        return getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
    }

    private String getKey(String sourceTable, String sourceType) {
        return sourceTable + ":" + sourceType;
    }

    private void deleteNotNeedColumns(JSONObject jsonObj, String needColumns) {
        List<String> columnList = Arrays.asList(needColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = jsonObj.entrySet();
        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}
