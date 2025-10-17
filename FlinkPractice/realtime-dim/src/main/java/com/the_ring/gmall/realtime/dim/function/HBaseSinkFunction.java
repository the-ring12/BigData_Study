package com.the_ring.gmall.realtime.dim.function;

import com.alibaba.fastjson2.JSONObject;
import com.the_ring.gmall.realtime.common.bean.TableProcessDim;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Description 将结果写入 HBase 表
 * @Date 2025/10/16
 * @Author the_ring
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection connection;


    @Override
    public void open(OpenContext openContext) throws Exception {
        connection = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        Object type = jsonObject.get("type");
        jsonObject.remove("type");
        if ("delete".equals(type)) {
            // HBase 中对应删除
            HBaseUtil.deleteRow(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkRowKey());
        } else {
            // HBase 中 put 数据
            HBaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), jsonObject.getString(tableProcessDim.getSinkRowKey()), tableProcessDim.getSinkFamily(), jsonObject);
        }
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(connection);
    }
}
