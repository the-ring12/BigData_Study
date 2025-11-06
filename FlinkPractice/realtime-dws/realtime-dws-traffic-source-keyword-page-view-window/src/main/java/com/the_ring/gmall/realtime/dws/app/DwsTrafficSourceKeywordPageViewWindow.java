package com.the_ring.gmall.realtime.dws.app;

import com.the_ring.gmall.realtime.common.base.BaseSQLApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.SQLUtil;
import com.the_ring.gmall.realtime.dws.function.TokenizationFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description 搜索关键词分词统计
 * @Date 2025/11/5
 * @Author the_ring
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 4, "dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 读取页面日志
        tEnv.executeSql("CREATE TABLE page_log (\n" +
                " page MAP<string, string>,\n" +
                " ts BIGINT,\n" +
                " et AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                " WATERMARK FOR et AS et - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_traffic_keyword_page_view_window"));

        Table keywordTable = tEnv.sqlQuery("SELECT \n" +
                " page['item'] kw,\n" +
                " et\n" +
                "FROM page_log\n" +
                "WHERE (page['last_page_id'] = 'search' or page['last_page_id'] = 'home')\n" +
                " AND page['item_type'] = 'keyword'\n" +
                " AND page['item'] IS NOT NULL");
        tEnv.createTemporaryView("keyword", keywordTable);
//        tEnv.executeSql("SELECT * FROM keyword_table").print();

        // 自定义分词函数
        tEnv.createTemporaryFunction("tokenization", TokenizationFunction.class);
        Table woredTable = tEnv.sqlQuery("SELECT word, et\n" +
                "FROM keyword\n" +
                "JOIN LATERAL TABLE(tokenization(kw)) AS T(word) ON TRUE");
        tEnv.createTemporaryView("word", woredTable);
//        tEnv.executeSql("SELECT * FROM word").print();

        // 开窗
        Table resultTable = tEnv.sqlQuery("SELECT \n" +
                " DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') sst, \n" +
                " DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                " DATE_FORMAT(window_start, 'yyyy-MM-dd') cur_date,\n" +
                " word keyword, \n" +
                " count(*) AS keyword_count\n" +
                "FROM TABLE(\n" +
                "  TUMBLE(TABLE word, DESCRIPTOR(et), INTERVAL '5' SECONDS))\n" +
                "GROUP BY window_start, window_end, word");
        resultTable.execute().print();

        // 结果写入 Doris
        tEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                " stt string,\n" +
                " edt string,\n" +
                " cur_date string,\n" +
                " keyword string,\n" +
                " keyword_count BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constant.DORIS_FE_NODES +"',\n" +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                "  'sink.properties.format' = 'json',\n" +
                "  'sink.buffer-count' = '4',\n" +
                "  'sink.buffer-size' = '4086',\n" +
                "  'sink.enable-2pc' = 'false',\n" +
                "  'sink.properties.read_json-by_line' = 'true'\n" +
                ")");
        resultTable.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
