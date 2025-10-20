package com.the_ring.gmall.realtime.dwd.log.split;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.the_ring.gmall.realtime.common.base.BaseApp;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.DateFormatUtil;
import com.the_ring.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description 日志分流
 * @Date 2025/10/17
 * @Author the_ring
 */
public class DwdBaseLog extends BaseApp {


    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";


    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", "topic-log");
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 1. 数据类型转换并做 ETL
        SingleOutputStreamOperator<JSONObject> jsonObj = etl(kafkaStrDS);

        // 2. 对新老访客标记修复
        SingleOutputStreamOperator<JSONObject> fixedDS = validateNewOrOld(jsonObj);

        // 3. 分流：错误、启动、曝光、动作、页面（主流）
        Map<String, DataStream<JSONObject>> streamMap = splitStream(fixedDS);
        // 不同流写到不同 Kafka
        writeToKafka(streamMap);

    }

    // 把各个流分别写入不同的 Kafka 主题
    public void writeToKafka(Map<String, DataStream<JSONObject>> streamMap) {
        streamMap.get(ERR)
                .map((MapFunction<JSONObject, String>) value -> value.toString())
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        streamMap.get(START)
                .map((MapFunction<JSONObject, String>) value -> value.toString())
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));

        streamMap.get(DISPLAY)
                .map((MapFunction<JSONObject, String>) value -> value.toString())
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));

        streamMap.get(ACTION)
                .map((MapFunction<JSONObject, String>) value -> value.toString())
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

        streamMap.get(PAGE)
                .map((MapFunction<JSONObject, String>) value -> value.toString())
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
    }


    // 就正 "is_new" 字段
    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> jsonObj) {
        // 根据设备 id 分组
        return jsonObj.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                // 使用状态编程完成修复
                .map(
                        new RichMapFunction<JSONObject, JSONObject>() {
                            ValueState<String> firstVisitDateState;

                            @Override
                            public void open(OpenContext openContext) throws Exception {
                                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                                firstVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public JSONObject map(JSONObject jsonObject) throws Exception {
                                JSONObject common = jsonObject.getJSONObject("common");
                                String isNew = common.getString("is_new");
                                Long ts = jsonObject.getLong("ts");
                                String tsDate = DateFormatUtil.tsToDate(ts);
                                String firstVisitDate = firstVisitDateState.value();
                                /**
                                 * 老访客就一定是老访客
                                 * 标记为新访客的，因为内部缓存原因或不同设备，可能会出现误标记
                                 * 因为数据是时间顺序的，不用考虑先后问题
                                 */
                                if ("1".equals(isNew)) {
                                    if (StringUtils.isEmpty(firstVisitDate)) {
                                        // 上次访问为空，不做修改，并记录值
                                        firstVisitDateState.update(tsDate);
                                    } else if (!tsDate.equals(firstVisitDate)) {
                                        common.put("is_new", "0");
                                    }
                                } else {
                                    if (StringUtils.isEmpty(firstVisitDate)) {
                                        // 将日期更新为昨天
                                        firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 100));
                                    }
                                }
                                return jsonObject;
                            }
                        }
                );
    }

    // 根据日志内容把日志分别输出到侧流，主流为 页面数据
    public Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        // 定义侧输出流
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("errTag") {
        };
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("startTag") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("displayTag") {
        };
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("actionTag") {
        };
        SingleOutputStreamOperator<JSONObject> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (jsonObject.get("err") != null) {
                            // 错判日志
                            ctx.output(errTag, jsonObject);
                            jsonObject.remove("err");
                        }

                        if (jsonObject.get("start") != null) {
                            ctx.output(startTag, jsonObject);
                        } else {
                            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");

                            // 曝光
                            JSONArray displayArray = jsonObject.getJSONArray("displays");
                            if (displayArray != null && !displayArray.isEmpty()) {
                                for (int i = 0; i < displayArray.size(); i++) {
                                    JSONObject displayObj = displayArray.getJSONObject(i);
                                    JSONObject newJsonObj = new JSONObject();
                                    newJsonObj.put("common", commonJsonObj);
                                    newJsonObj.put("page", pageJsonObj);
                                    newJsonObj.put("display", displayObj);
                                    newJsonObj.put("ts", ts);
                                    ctx.output(displayTag, newJsonObj);
                                }
                            }
                            jsonObject.remove("displays");

                            // 行为日志
                            JSONArray actionsArray = jsonObject.getJSONArray("actions");
                            if (actionsArray != null && !actionsArray.isEmpty()) {
                                for (int i = 0; i < actionsArray.size(); i++) {
                                    JSONObject actionObj = actionsArray.getJSONObject(i);
                                    JSONObject newJsonObj = new JSONObject();
                                    newJsonObj.put("common", commonJsonObj);
                                    newJsonObj.put("page", pageJsonObj);
                                    newJsonObj.put("action", actionObj);
                                    ctx.output(actionTag, newJsonObj);
                                }
                            }
                            jsonObject.remove("actions");

                            // 页面日志 写入主流
                            out.collect(jsonObject);
                        }
                    }
                }
        );
        Map<String, DataStream<JSONObject>> streamMap = new HashMap<>();
        streamMap.put(START, pageDS.getSideOutput(startTag));
        streamMap.put(ERR, pageDS.getSideOutput(errTag));
        streamMap.put(DISPLAY, pageDS.getSideOutput(displayTag));
        streamMap.put(ACTION, pageDS.getSideOutput(actionTag));
        streamMap.put(PAGE, pageDS);

        return streamMap;
    }

    // 输出处理，过滤脏数据数据并输出到 Kafka
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        // 定义侧输出流
        //        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){}; // 需要加括号否则编译会被泛型擦除，获取不到类型,或者使用下面的方式
        OutputTag<String> dirtyTag = new OutputTag<>("dirtyTag", TypeInformation.of(String.class));
        // ETL
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            // 不是标准 json，放入 侧输出流
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        //        jsonObj.print("标准的 json: ");
        SideOutputDataStream<String> dirtyDS = jsonObj.getSideOutput(dirtyTag);
        //        dirtyDS.print("脏数据:");
        // 侧输出流输出到 Kafka
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty-data");
        dirtyDS.sinkTo(kafkaSink);

        return jsonObj;
    }
}
