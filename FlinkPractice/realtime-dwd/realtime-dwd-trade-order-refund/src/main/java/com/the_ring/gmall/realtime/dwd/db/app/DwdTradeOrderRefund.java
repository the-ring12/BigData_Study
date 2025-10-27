package com.the_ring.gmall.realtime.dwd.db.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description 退单数据流
 * @Date 2025/10/27
 * @Author the_ring
 */
public class DwdTradeOrderRefund extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017, 4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

        readHBaseBaseDic(tEnv);

        Table orderRefundInfo = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['order_id'] order_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " `data`['refund_type'] refund_type,\n" +
                " `data`['refund_num'] refund_num,\n" +
                " `data`['refund_amount'] refund_amount,\n" +
                " `data`['refund_reason_type'] refund_reason_type,\n" +
                " `data`['refund_reason_txt'] refund_reason_txt,\n" +
                " `data`['create_time'] create_time,\n" +
                " `pt`,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_refund_info'\n" +
                "AND `type` = 'insert' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        Table orderInfo = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['province_id'] province_id,\n" +
                " `data`['create_time'] create_time,\n" +
                " `old` \n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'update' \n" +
                "AND `old`['order_status'] IS NOT NULL \n" +
                "AND `data`['order_status'] = '1005' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        Table result = tEnv.sqlQuery("SELECT \n" +
                " ri.id,\n" +
                " ri.user_id,\n" +
                " ri.order_id,\n" +
                " ri.sku_id,\n" +
                " oi.province_id,\n" +
                " CAST(DATE_FORMAT(ri.create_time, 'yyyy-MM-dd') AS string) date_id,\n" +
                " ri.create_time,\n" +
                " ri.refund_type refund_code_type,\n" +
                " dic1.info.dic_name refund_code_name,\n" +
                " ri.refund_reason_type refund_reason_type_code,\n" +
                " dic2.info.dic_name refund_reason_type_name,\n" +
                " ri.refund_reason_txt,\n" +
                " ri.refund_num,\n" +
                " ri.refund_amount,\n" +
                " ri.ts \n" +
                "FROM order_refund_info ri \n" +
                "JOIN order_info oi ON ri.order_id = oi.id \n" +
                "JOIN base_dic  FOR SYSTEM_TIME AS OF ri.pt AS dic1 " +
                "ON ri.refund_type = dic1.dic_code \n" +
                "JOIN base_dic  FOR SYSTEM_TIME AS OF ri.pt AS dic2 " +
                "ON ri.refund_reason_type = dic2.dic_code");

        // 发送数据到 Kafka
        tEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_REFUND + " (\n" +
                " id string,\n" +
                " user_id string,\n" +
                " order_id string,\n" +
                " sku_id string,\n" +
                " province_id string,\n" +
                " date_id string,\n" +
                " create_time string,\n" +
                " refund_type_code string,\n" +
                " refund_code_name string,\n" +
                " refund_reason_type_code string,\n" +
                " refund_reason_type_name string,\n" +
                " refund_reason_txt string,\n" +
                " refund_num string,\n" +
                " refund_amount string,\n" +
                " ts bigint, \n" +
                " PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
}
