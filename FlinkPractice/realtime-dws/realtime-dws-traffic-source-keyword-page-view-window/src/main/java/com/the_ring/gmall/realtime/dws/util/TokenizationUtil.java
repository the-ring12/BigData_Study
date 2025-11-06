package com.the_ring.gmall.realtime.dws.util;

import com.hankcs.hanlp.HanLP;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description 分词工具类
 * @Date 2025/11/5
 * @Author the_ring
 */
public class TokenizationUtil {

    public static List<String> segmentChinese(String text) {
        return HanLP.segment(text).stream()
                .filter(term -> !term.nature.toString().startsWith("w"))
                .map(term -> term.word)
                .collect(Collectors.toList());
    }
}
