package com.the_ring.gmall.realtime.common.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Description 日期处理工具类
 * @Date 2025/10/20
 * @Author the_ring
 */
public class DateFormatUtil {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfForPartition = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 年月日时分秒转换为毫秒
     * @param dateTime 完整日期格式
     * @return 毫秒
     */
    public static Long dateTimeToTs(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, dtfFull);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 毫秒转化为年月日
     * @param ts 毫秒
     * @return 年月日
     */
    public static String tsToDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    /**
     * 毫秒转 年月日时分秒
     * @param ts 毫秒
     * @return 年月日时分秒
     */
    public static String tsToDateTime(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    /**
     * 毫秒 转 年月日分区格式 (yyyyMMdd)
     * @param ts
     * @return
     */
    public static String tsToDateForPartition(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfForPartition.format(localDateTime);
    }

    /**
     * 年月日 转 毫秒
     * @param date 年月日
     * @return 毫秒
     */
    public static long dateToTs(String date) {
        return dateToTs(date + " 00:00:00");
    }

    /**
     * 判断两个 年月日的字符串日期
     * @param date1 年月日 1
     * @param date2 年月日 2
     * @return date1 晚于 date 2 返回 > 0，同一天返回 0，早于返回 < 0
     */
    public static int compareDate(String date1, String date2) {
        LocalDateTime localDate1 = LocalDateTime.parse(date1, dtf);
        LocalDateTime localDate2 = LocalDateTime.parse(date2, dtf);
        return localDate1.compareTo(localDate2);
    }

}
