package com.the_ring.gmall.realtime.common.util;

import com.google.common.base.CaseFormat;
import com.the_ring.gmall.realtime.common.constant.Constant;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description 通过 JDBC 操作 MySQL 工具类
 * @Date 2025/10/17
 * @Author the_ring
 */
public class JDBCUtil {

    public static Connection getMysqlConnection(String database) throws Exception {
        Class.forName(Constant.MYSQL_DRIVER);
        return DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
    }

    public static void closeMysqlConnection(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public static <T>List<T> queryList(Connection connection, String sql, Class<T> clz, boolean... isUnderlineTpCamel) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ArrayList<T> resList = new ArrayList<>();
        boolean defaultIsUnderlineToCamel = false;

        if (isUnderlineTpCamel.length > 0) {
            defaultIsUnderlineToCamel = isUnderlineTpCamel[0];
        }


        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        while (resultSet.next()) {
            T obj = clz.getDeclaredConstructor().newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = resultSet.getObject(i);
                if (defaultIsUnderlineToCamel) {
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(obj, columnName, columnValue);
            }
            resList.add(obj);
        }
        resultSet.close();
        statement.close();
        
        return resList;
    }
}
