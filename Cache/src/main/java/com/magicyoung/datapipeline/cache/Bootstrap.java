package com.magicyoung.datapipeline.cache;

import com.magicyoung.datapipelines.common.utils.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: magicyoung
 * @Date: 2019/3/19 13:51
 * @Description:启动缓存客户端，向redis中增加缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {
        // 1. 读取MySQL中的数据
        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();

        // 读取用户和时间的数据
        String queryUserSql = "select id, tel from ct_user";
        String queryDateSql = "select id, year, month, day from ct_date";
        Connection connection = null;
        PreparedStatement pstat = null;
        ResultSet resultSet = null;
        try {
            connection = JDBCUtil.getConnection();
            pstat = connection.prepareStatement(queryUserSql);
            resultSet = pstat.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String tel = resultSet.getString(2);
                userMap.put(tel, id);
            }
            resultSet.close();

            pstat = connection.prepareStatement(queryDateSql);
            resultSet = pstat.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String year = resultSet.getString(2);
                String month = resultSet.getString(3);
                String day = resultSet.getString(4);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                if (day.length() == 1) {
                    day = "0" + day;
                }
                userMap.put(year + month + day, id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstat != null) {
                try {
                    pstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        // 2. 向redis中存储数据
        Jedis jedis = new Jedis("master", 6379);
        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user", key, "" + value);
        }

        keyIterator = dateMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = dateMap.get(key);
            jedis.hset("ct_date", key, "" + value);
        }
    }
}
