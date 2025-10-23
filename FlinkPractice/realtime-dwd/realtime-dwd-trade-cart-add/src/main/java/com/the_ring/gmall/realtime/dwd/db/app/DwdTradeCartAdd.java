package com.the_ring.gmall.realtime.dwd.db.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description 加购数据
 * @Date 2025/10/23
 * @Author the_ring
 */
public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);

        // 过滤加购数据
        Table cartAdd = tEnv.sqlQuery("SELECT \n" +
                " `data`['id'] id,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " IF(`type` = 'insert', \n" +
                "  `data`['sku_num'],\n" +
                "  CAST((CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS STRING)\n" +
                " ) sku_num,\n" +
                " ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'cart_info'\n" +
                "AND (\n" +
                " `type` = 'insert' \n" +
                " OR (\n" +
                "  `type` = 'update' \n" +
                "  AND `old`['sku_num'] IS NOT NULL \n" +
                "  AND CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)\n" +
                " )\n" +
                ")");

        // 写出到 Kafka
        tEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + " (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  sku_num string,\n" +
                "  `ts` bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

        cartAdd.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

    }
}
