package com.the_ring.gmall.realtime.common;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.PropertyNamingStrategy;
import com.alibaba.fastjson2.writer.ObjectWriterProvider;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Description bean 数据转换
 * @Date 2025/11/7
 * @Author the_ring
 * bean 字段为驼峰命名，Doris 使用下划线，此类用于完成命名转换
 */
public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        JSONWriter.Context context = new JSONWriter.Context(new ObjectWriterProvider(PropertyNamingStrategy.SnakeCase));
        return JSON.toJSONString(bean, context);
    }
}
