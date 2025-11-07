package com.the_ring.gmall.realtime.common.bean;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description 访客实体类
 * @Date 2025/11/7
 * @Author the_ring
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrafficPageViewBean {
    /**
     * 窗口起始时间
     */
    private String stt;
    /**
     * 窗口结束时间
     */
    private String edt;
    /**
     * 当天时间
     */
    private String curDate;
    /**
     * app 版本号
     */
    private String version;
    /**
     * 渠道
     */
    private String channel;
    /**
     * 地区
     */
    private String region;
    /**
     * 新老访客标记
     */
    private String isNew;
    /**
     * 独立访客数
     */
    private Long uvCount;
    /**
     * 会话数
     */
    private Long svCount;
    /**
     * 页面浏览数
     */
    private Long pvCount;
    /**
     * 累计访问时长
     */
    private Long durationSum;
    /**
     * 时间戳
     */
    @JSONField(serialize = false)
    private Long ts;
    @JSONField(serialize = false)
    private String sid;

}
