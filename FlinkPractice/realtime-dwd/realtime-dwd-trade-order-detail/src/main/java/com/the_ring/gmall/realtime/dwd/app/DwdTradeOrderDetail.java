package com.the_ring.gmall.realtime.dwd.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description 下单事实流
 * @Date 2025/10/24
 * @Author the_ring
 */
public class DwdTradeOrderDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10013, 4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 设置状态的保留时间 [传输的延迟 + 业务上的滞后关系]
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        // 订单明细表
        Table orderDetail = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['order_id'] order_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " `data`['sku_name'] sku_name,\n" +
                " `data`['create_time'] create_time,\n" +
                " `data`['source_id'] source_id,\n" +
                " `data`['source_type'] source_type,\n" +
                " `data`['sku_num'] sku_num,\n" +
                " CAST(CAST(`data`['sku_num'] AS DECIMAL(16,2)) * CAST(`data`['order_price'] AS DECIMAL(16,2)) AS string) AS split_original_amount,\n" +
                " `data`['split_total_amount'] split_total_amount,\n" +
                " `data`['split_activity_amount'] split_activity_amount,\n" +
                " `data`['split_coupon_amount'] split_coupon_amount,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_detail'\n" +
                "AND `type` = 'insert' ");
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 订单表
        Table orderInfo = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['province_id'] province_id,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'insert' ");
        tEnv.createTemporaryView("order_info", orderInfo);


        // 订单活动表
        Table orderDetailActivity = tEnv.sqlQuery("SELECT \n" +
                " `data`['order_detail_id'] order_detail_id,\n" +
                " `data`['activity_id'] activity_id,\n" +
                " `data`['activity_rule_id'] activity_rule_id,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_detail_activity'\n" +
                "AND `type` = 'insert' ");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 订单优惠劵表
        Table orderDetailCoupon = tEnv.sqlQuery("SELECT \n" +
                " `data`['order_detail_id'] order_detail_id,\n" +
                " `data`['coupon_id'] coupon_id,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_detail_coupon'\n" +
                "AND `type` = 'insert' ");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        Table result = tEnv.sqlQuery("SELECT \n" +
                " od.id,\n" +
                " od.order_id,\n" +
                " oi.user_id,\n" +
                " od.sku_id,\n" +
                " od.sku_name,\n" +
                " oi.province_id,\n" +
                " act.activity_id,\n" +
                " act.activity_rule_id,\n" +
                " cou.coupon_id,\n" +
                " DATE_FORMAT(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                " od.create_time,\n" +
                " od.sku_num,\n" +
                " od.split_original_amount,\n" +
                " od.split_activity_amount,\n" +
                " od.split_coupon_amount,\n" +
                " od.split_total_amount,\n" +
                " od.ts\n" +
                "FROM order_detail od\n" +
                "JOIN order_info oi ON od.order_id=oi.id\n" +
                "LEFT JOIN order_detail_activity act ON od.id=act.order_detail_id\n" +
                "LEFT JOIN order_detail_coupon cou ON od.id=cou.order_detail_id");
//        result.execute().print();

        // 发送数据到 Kafka
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
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        result.executeInsert("dwd_trade_order_detail");

    }
}
