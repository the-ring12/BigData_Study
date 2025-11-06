package com.the_ring.gmall.realtime.dws.function;

import com.the_ring.gmall.realtime.dws.util.TokenizationUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @Description 自定义的分词函数，SQL 中可调用
 * @Date 2025/11/5
 * @Author the_ring
 */
@FunctionHint(output = @DataTypeHint("row<keyword string>"))
public class TokenizationFunction extends TableFunction<Row> {

    public void eval(String text) {
        if (text == null) return;
        List<String> keywords = TokenizationUtil.segmentChinese(text);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }
}
