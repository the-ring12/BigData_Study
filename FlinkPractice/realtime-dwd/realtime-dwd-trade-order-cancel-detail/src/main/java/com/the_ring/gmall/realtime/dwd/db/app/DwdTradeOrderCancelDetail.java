package com.the_ring.gmall.realtime.dwd.db.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description 订单取消
 * @Date 2025/10/24
 * @Author the_ring
 */
public class DwdTradeOrderCancelDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10014, 4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));

        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

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
                " ts bigint\n" +
                ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 订单表
        Table cancelOrderInfo = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['operate_time'] operate_time,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'update' \n" +
                "AND `data`['order_status'] = '1003'\n" +
                "AND `data`['order_status'] <> `old`['order_status']");
        tEnv.createTemporaryView("cancel_order_info", cancelOrderInfo);


        Table result = tEnv.sqlQuery("SELECT \n" +
                " od.id,\n" +
                " od.order_id,\n" +
                " od.user_id,\n" +
                " od.sku_id,\n" +
                " od.sku_name,\n" +
                " od.province_id,\n" +
                " od.activity_id,\n" +
                " od.activity_rule_id,\n" +
                " od.coupon_id,\n" +
                " CAST(DATE_FORMAT(oc.operate_time, 'yyyy-MM-dd') AS STRING) order_cancel_date_id,\n" +
                " oc.operate_time,\n" +
                " od.sku_num,\n" +
                " od.split_original_amount,\n" +
                " od.split_activity_amount,\n" +
                " od.split_coupon_amount,\n" +
                " od.split_total_amount,\n" +
                " od.ts\n" +
                "FROM dwd_trade_order_detail od\n" +
                "JOIN cancel_order_info oc ON od.id=oc.id");
//        result.execute().print();

        // 发送数据到 Kafka
        tEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + " (\n" +
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
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);


    }
}
