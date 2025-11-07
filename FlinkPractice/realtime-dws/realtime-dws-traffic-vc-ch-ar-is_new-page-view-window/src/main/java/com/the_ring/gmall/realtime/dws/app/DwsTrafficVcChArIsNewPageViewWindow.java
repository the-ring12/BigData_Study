package com.the_ring.gmall.realtime.dws.app;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.the_ring.gmall.realtime.common.DorisMapFunction;
import com.the_ring.gmall.realtime.common.base.BaseApp;
import com.the_ring.gmall.realtime.common.bean.TrafficPageViewBean;
import com.the_ring.gmall.realtime.common.constant.Constant;
import com.the_ring.gmall.realtime.common.util.DateFormatUtil;
import com.the_ring.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @Description 版本-渠道-地区-访客类别粒度页面浏览数据统计
 * @Date 2025/11/7
 * @Author the_ring
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {

    private static final String name = "dws_traffic_vc_ch_ar_is_new_page_view_window";

    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022, 4, name, Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 转化为对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = kafkaStrDS.map(JSON::parseObject)
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        lastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        JSONObject page = value.getJSONObject("page");
                        JSONObject common = value.getJSONObject("common");
                        Long ts = value.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        Long pv = 1L;
                        Long duration = page.getLong("during_time");

                        // uv
                        String lastVisitDate = lastVisitDateState.value();
                        Long uv = 0L;
                        if (!today.equals(lastVisitDate)) {
                            // 今天第一次访问
                            uv = 1L;
                            lastVisitDateState.update(today);
                        }

                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean();
                        trafficPageViewBean.setVersion(common.getString("vc"));
                        trafficPageViewBean.setChannel(common.getString("ch"));
                        trafficPageViewBean.setRegion(common.getString("ar"));
                        trafficPageViewBean.setIsNew(common.getString("is_new"));
                        trafficPageViewBean.setSid(common.getString("sid"));

                        trafficPageViewBean.setPvCount(pv);
                        trafficPageViewBean.setUvCount(uv);
                        trafficPageViewBean.setDurationSum(duration);
                        trafficPageViewBean.setTs(ts);


                        out.collect(trafficPageViewBean);
                    }
                })
                .keyBy(TrafficPageViewBean::getSid)
                .process(new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {

                    private ValueState<Boolean> isFirstState;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<Boolean>("isFirst", Boolean.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofHours(1))
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .useProcessingTime()
                                .build();
                        descriptor.enableTimeToLive(ttlConfig);
                        isFirstState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(TrafficPageViewBean trafficPageViewBean, KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        if (isFirstState.value() == null) {
                            trafficPageViewBean.setSvCount(1L);
                            isFirstState.update(true);
                        } else {
                            trafficPageViewBean.setSvCount(0L);
                        }
                        out.collect(trafficPageViewBean);
                    }
                });

        // 开窗函数
        SingleOutputStreamOperator<TrafficPageViewBean> result =
                beanDS.assignTimestampsAndWatermarks(
                                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                        .withTimestampAssigner((bean, ts) -> bean.getTs())
                                        .withIdleness(Duration.ofSeconds(120L)))
                        .keyBy(bean -> bean.getVersion() + "_" + bean.getChannel() + "_" + bean.getRegion() + "_" + bean.getIsNew())
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5L)))
                        .reduce(
                                new ReduceFunction<TrafficPageViewBean>() {
                                    @Override
                                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                                        value1.setPvCount(value1.getPvCount() + value2.getPvCount());
                                        value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                                        value1.setSvCount(value1.getSvCount() + value2.getSvCount());
                                        value1.setDurationSum(value1.getDurationSum() + value2.getDurationSum());

                                        return value1;
                                    }
                                },
                                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                                    @Override
                                    public void process(String s, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                                        TrafficPageViewBean bean = elements.iterator().next();
                                        bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                                        bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                                        bean.setCurDate(DateFormatUtil.tsToDateForPartition(context.window().getStart()));

                                        out.collect(bean);
                                    }
                                });

        // 写入 Doris
        result.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(name, Constant.DORIS_DATABASE + "." + name));


    }
}
