package com.the_ring.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Description 配置表实体类
 * @Date 2025/10/11
 * @Author the_ring
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDim {

    /**
     * 来源表名
     */
    String sourceTable;

    /**
     * 目标表名
     */
    String sinkTable;

    /**
     * 输出字段
     */
    String sinkColumns;

    /**
     * 目标 HBase 列族名
     */
    String sinkFamily;

    /**
     * 目标 HBase 行键字段
     */
    String sinkRowKey;

    /**
     * 操作类型
     */
    String op;

}
