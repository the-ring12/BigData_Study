package com.the_ring.gmall.realtime.common.util;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @Description HBase 工具类
 * @Date 2025/10/11
 * @Author the_ring
 */
@Slf4j
public class HBaseUtil {

    public static Connection getHBaseConnection() throws IOException {
        Configuration config = new Configuration();
        config.set("hbase.zookeeper.quorum", "hadoop00,hadoop01,hadoop02");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(config);
    }

    public static void closeHBaseConnection(Connection connection) throws IOException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public static void createHBaseTable(Connection connection, String nameSpace, String tableName, String family) throws IOException {
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(nameSpace, tableName);
        if (admin.tableExists(table)) {
            log.warn("表空间下 " + nameSpace + " 的表 " + tableName + " 已存在，创建失败！");
            return;
        }
        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(family);
        TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(table)
                .setColumnFamily(familyDescriptor)
                .build();
        admin.createTable(descriptor);
        log.info("表空间下 " + nameSpace + " 的表 " + tableName + "创建成功！");
    }

    public static void dropHbaseTable(Connection connection, String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(nameSpace, tableName);
        if (!admin.tableExists(table)) {
            log.warn("表空间下 " + nameSpace + " 的表 " + tableName + " 不存在，删除失败！");
            return;
        }
        admin.disableTable(table);
        admin.deleteTable(table);
        log.info("表空间下 " + nameSpace + " 的表 " + tableName + " 删除成功！");
    }

}
