package com.the_ring.gmall.realtime.common.util;


import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;

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
            System.err.println("表空间下 " + nameSpace + " 的表 " + tableName + " 已存在，创建失败！");
            return;
        }
        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(family);
        TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(table)
                .setColumnFamily(familyDescriptor)
                .build();
        admin.createTable(descriptor);
        System.out.println("表空间下 " + nameSpace + " 的表 " + tableName + " 创建成功！");
    }

    public static void dropHbaseTable(Connection connection, String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(nameSpace, tableName);
        if (!admin.tableExists(table)) {
            System.err.println("表空间下 " + nameSpace + " 的表 " + tableName + " 不存在，删除失败！");
            return;
        }
        if (admin.isTableEnabled(table)) {
            admin.disableTable(table);
        }
        admin.deleteTable(table);
        System.out.println("表空间下 " + nameSpace + " 的表 " + tableName + " 删除成功！");
    }

    /**
     * HBase 中插入数据
     * @param connection 连接对象
     * @param nameSpace 表空间
     * @param tableName 表
     * @param rowKey 行键
     * @param family 列族
     * @param jsonObject 待插入数据
     */
    public static void putRow(Connection connection, String nameSpace, String tableName, String rowKey, String family, JSONObject jsonObject) {
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = connection.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObject.keySet();
            byte[] familyBytes = Bytes.toBytes(family);
            for (String column : columns) {
                String value = jsonObject.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(familyBytes, Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }

            table.put(put);
            System.out.println("表空间下 " + nameSpace + " 的表 " + tableName + " 插入一条数据！");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * HBase 中删除一条数据
     * @param connection 连接对象
     * @param nameSpace 命名空间
     * @param tableName 表
     * @param rowKey 行键
     */
    public static void deleteRow(Connection connection, String nameSpace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = connection.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            log.info("表空间下 " + nameSpace + " 的表 " + tableName + " 删除一条数据！");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtil.getHBaseConnection();
        TableName tableNameObj = TableName.valueOf("gmall", "dim_base_dic");
        try (Table table = connection.getTable(tableNameObj);
             ResultScanner scanner = table.getScanner(new Scan());) {
            for (Result resultRow : scanner) {
                String rowKey = Bytes.toString(resultRow.getRow());
                System.out.print(rowKey + ": ");
                for (Cell cell : resultRow.listCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    long timestamp = cell.getTimestamp();
                    System.out.print(family + "=>" + column + "=" + value + ", ");
                }
                System.out.println();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
