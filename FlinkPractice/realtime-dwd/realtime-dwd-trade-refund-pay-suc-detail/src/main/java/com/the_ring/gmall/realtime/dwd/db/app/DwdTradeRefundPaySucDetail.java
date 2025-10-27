package com.the_ring.gmall.realtime.dwd.db.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Description 退款成功
 * @Date 2025/10/27
 * @Author the_ring
 */
public class DwdTradeRefundPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018, 4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
        readHBaseBaseDic(tEnv);

        // 退款成功表数据
        Table refundPayment = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['order_id'] order_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " `data`['payment_type'] payment_type,\n" +
                " `data`['callback_time'] callback_time,\n" +
                " `data`['total_amount'] total_amount,\n" +
                " `pt`,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'refund_payment'\n" +
                "AND `type` = 'update'\n" +
                "AND `old`['refund_status'] IS NOT NULL\n" +
                "AND `data`['refund_status'] = '1602'" );
        tEnv.createTemporaryView("refund_payment", refundPayment);

        // 退单表退单成功
        Table orderRefundInfo = tEnv.sqlQuery("SELECT \n" +
                " `data`['order_id'] order_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " `data`['refund_num'] refund_num\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_refund_info'\n" +
                "AND `type` = 'update' \n" +
                "AND `old`['refund_status'] IS NOT NULL \n" +
                "AND `data`['refund_status'] = '0705' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 订单表表退款成功
        Table orderInfo = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['province_id'] province_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'update' \n" +
                "AND `old`['order_status'] IS NOT NULL \n" +
                "AND `data`['order_status'] = '1006' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        Table result = tEnv.sqlQuery("SELECT \n" +
                " rp.id,\n" +
                " oi.user_id,\n" +
                " rp.order_id,\n" +
                " rp.sku_id,\n" +
                " oi.province_id,\n" +
                " rp.payment_type payment_type_code,\n" +
                " dic.info.dic_name payment_type_name,\n" +
                " CAST(DATE_FORMAT(rp.callback_time, 'yyyy-MM-dd') AS string) date_id,\n" +
                " rp.callback_time,\n" +
                " ori.refund_num,\n" +
                " rp.total_amount,\n" +
                " rp.ts \n" +
                "FROM refund_payment rp \n" +
                "JOIN order_refund_info ori ON rp.order_id = ori.order_id AND rp.sku_id = ori.sku_id \n" +
                "JOIN order_info oi ON rp.order_id = oi.id \n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF rp.pt AS dic " +
                "ON rp.payment_type = dic.dic_code");

        // 发送数据到 Kafka
        tEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS + " (\n" +
                " id string,\n" +
                " user_id string,\n" +
                " order_id string,\n" +
                " sku_id string,\n" +
                " province_id string,\n" +
                " payment_type_code string,\n" +
                " payment_type_name string,\n" +
                " date_id string,\n" +
                " callback_time string,\n" +
                " refund_num string,\n" +
                " refund_amount string,\n" +
                " ts bigint, \n" +
                " PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
}
