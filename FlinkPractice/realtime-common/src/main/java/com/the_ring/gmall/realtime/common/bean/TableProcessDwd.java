package com.the_ring.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description 实体类
 * @Date 2025/10/28
 * @Author the_ring
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    /**
     * 来源表名
     */
    String sourceTable;

    /**
     * 来源类型
     */
    String sourceType;

    /**
     * 目标表名
     */
    String sinkTable;

    /**
     * 输出字段
     */
    String sinkColumns;

    /**
     * 配置表操作类型
     */
    String op;
}
