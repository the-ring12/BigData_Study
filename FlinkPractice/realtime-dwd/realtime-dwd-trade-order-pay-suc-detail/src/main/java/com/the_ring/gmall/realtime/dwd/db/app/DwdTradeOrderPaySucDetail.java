package com.the_ring.gmall.realtime.dwd.db.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description 下单支付成功
 * @Date 2025/10/27
 * @Author the_ring
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 读取 topic_db
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        // 读取 base_dic
        readHBaseBaseDic(tEnv);

        // 订单明细
        tEnv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
                " id string,\n" +
                " order_id string,\n" +
                " user_id string,\n" +
                " sku_id string,\n" +
                " sku_name string,\n" +
                " province_id string,\n" +
                " activity_id string,\n" +
                " activity_rule_id string,\n" +
                " coupon_id string,\n" +
                " date_id string,\n" +
                " create_time string,\n" +
                " sku_num string,\n" +
                " split_original_amount string,\n" +
                " split_activity_amount string,\n" +
                " split_coupon_amount string,\n" +
                " split_total_amount string,\n" +
                " ts bigint,\n" +
                " et AS TO_TIMESTAMP_LTZ(ts, 0), \n" +
                " WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
                ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        Table paymentInfo = tEnv.sqlQuery("SELECT \n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['order_id'] order_id,\n" +
                " `data`['payment_type'] payment_type,\n" +
                " `data`['callback_time'] callback_time,\n" +
                " `pt`,\n" +
                " ts,\n" +
                " TO_TIMESTAMP_LTZ(ts, 0) et\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'payment_info'\n" +
                "AND `type` = 'update' \n" +
                "AND `data`['payment_status'] = '1602'\n" +
                "AND `old`['payment_status'] IS NOT NULL");
        tEnv.createTemporaryView("payment_info", paymentInfo);

        Table result = tEnv.sqlQuery("SELECT \n" +
                " od.id order_detail_id,\n" +
                " od.order_id,\n" +
                " od.user_id,\n" +
                " od.sku_id,\n" +
                " od.sku_name,\n" +
                " od.province_id,\n" +
                " od.activity_id,\n" +
                " od.activity_rule_id,\n" +
                " od.coupon_id,\n" +
                " pi.payment_type payment_type_code,\n" +
                " dic.dic_name payment_type_name,\n" +
                " pi.callback_time,\n" +
                " od.sku_num,\n" +
                " od.split_original_amount,\n" +
                " od.split_activity_amount,\n" +
                " od.split_coupon_amount,\n" +
                " od.split_total_amount split_payment_amount,\n" +
                " pi.ts\n" +
                "FROM payment_info pi \n" +
                "JOIN dwd_trade_order_detail od ON pi.order_id = od.order_id " +
                "AND od.et >= pi.et -INTERVAL '30' MINUTE AND od.et <= pi.et -INTERVAL '5' SECOND\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF pi.pt AS dic ON pi.payment_type=dic.dic_code");

        // 发送数据到 Kafka
        tEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + " (\n" +
                " order_detail_id string,\n" +
                " order_id string,\n" +
                " user_id string,\n" +
                " sku_id string,\n" +
                " sku_name string,\n" +
                " province_id string,\n" +
                " activity_id string,\n" +
                " activity_rule_id string,\n" +
                " coupon_id string,\n" +
                " payment_type_code string,\n" +
                " payment_type_name string,\n" +
                " callback_time string,\n" +
                " sku_num string,\n" +
                " split_original_amount string,\n" +
                " split_activity_amount string,\n" +
                " split_coupon_amount string,\n" +
                " split_payment_amount string,\n" +
                " ts bigint,\n" +
                " PRIMARY KEY (order_detail_id) NOT ENFORCED\n" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

    }
}
