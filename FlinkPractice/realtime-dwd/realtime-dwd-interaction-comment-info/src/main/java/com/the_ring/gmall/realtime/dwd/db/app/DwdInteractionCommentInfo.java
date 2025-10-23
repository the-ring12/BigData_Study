package com.the_ring.gmall.realtime.dwd.db.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description 互动域评论
 * @Date 2025/10/21
 * @Author the_ring
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1. 建立动态表，从 topic_db 读取数据
        tEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `data` map<string, string>,\n" +
                "  `old` map<string, string>,\n" +
                "  `ts` bigint,\n" +
                "  `pt` as proctime()\n" +
                ") " + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        //        tEnv.sqlQuery("SELECT * FROM topic_db").execute().print();

        // 2. 过滤评论表数据
        Table commentInfo = tEnv.sqlQuery("SELECT \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "    `data`['create_time'] commnent_time,\n" +
                "    ts,\n" +
                "    pt\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'comment_info'\n" +
                "AND `type` = 'insert'");
        tEnv.createTemporaryView("comment_info", commentInfo);

        // 3. 通过 DDL 读取 HBase 中的 base_dic
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.6',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'hadoop00:2181,hadoop01:2181',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.partial-cache.max-rows' = '20',\n" +
                " 'lookup.partial-cache.expire-after-access' = '2 hour'\n" +
                " )");
//        tEnv.sqlQuery("SELECT * FROM base_dic").execute().print();

        // 4. 事实表和维度表进行 lookup join
        Table result = tEnv.sqlQuery("SELECT\n" +
                " ci.id,\n" +
                " ci.user_id,\n" +
                " ci.sku_id,\n" +
                " ci.appraise,\n" +
                " dic.info.dic_name appraise_name,\n" +
                " ci.comment_txt,\n" +
                " ci.ts\n" +
                "FROM comment_info ci\n" +
                "JOIN base_dic FOR system_time AS OF ci.pt AS dic\n" +
                "ON ci.appraise=dic.dic_code");
//        result.execute().print();

        // 5. 将结果输出到 Kafka topic
        tEnv.executeSql("CREATE TABLE dwd_interaction_comment_info (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  appraise string,\n" +
                "  appraise_name string,\n" +
                "  comment_txt string,\n" +
                "  ts bigint\n" +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        result.executeInsert("dwd_interaction_comment_info").print();  // 需要执行 print() 才能触发

    }
}
