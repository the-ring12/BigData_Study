package com.the_ring.gmall.realtime.dim.function;

import com.the_ring.gmall.realtime.common.bean.TableProcessDim;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Description 根据配置流信息在 HBase 中创建删除表
 * @Date 2025/10/16
 * @Author the_ring
 */
public class ConfigHBaseMapFunction extends RichMapFunction<TableProcessDim, TableProcessDim> {

    private Connection connection;


    @Override
    public void open(OpenContext openContext) throws Exception {
        connection = HBaseUtil.getHBaseConnection();
    }

    @Override
    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
        String op = tableProcessDim.getOp();
        if ("d".equals(op)) {
            HBaseUtil.dropHbaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
        } else if ("r".equals(op) || "c".equals(op)) {
            HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
        } else {
            HBaseUtil.dropHbaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
            HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
        }
        return tableProcessDim;
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(connection);
    }
}
