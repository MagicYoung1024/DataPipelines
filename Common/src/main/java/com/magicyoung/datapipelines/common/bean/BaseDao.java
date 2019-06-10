package com.magicyoung.datapipelines.common.bean;

import com.magicyoung.datapipelines.common.api.Column;
import com.magicyoung.datapipelines.common.api.Rowkey;
import com.magicyoung.datapipelines.common.api.TableRef;
import com.magicyoung.datapipelines.common.constant.Names;
import com.magicyoung.datapipelines.common.constant.ValueConstant;
import com.magicyoung.datapipelines.common.utils.DataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/3/5 18:03
 * @Description: 基础数据访问对象
 */
public abstract class BaseDao {
    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws IOException {
        getConnection();
        getAdmin();
    }

    protected void end() throws IOException {
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }

        Connection conn = getConnection();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }

    /**
     *  创建命名空间，如果存在则什么都不做
     * @param namespace
     */
    protected void createNamespaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();

        if (admin.getNamespaceDescriptor(namespace) == null) {
            // 创建命名空间描述器
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

            // 创建操作
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 创建表，如果有则覆盖
     * @param tabName
     * @param families
     */
    protected void createTableXX(String tabName, String coprocessorClass, String... families) throws IOException {
        createTableXX(tabName,null, null, families);
    }

    protected void createTableXX(String tabName, String coprocessorClass, Integer regionCount, String... families) throws IOException {
        Admin admin = getAdmin();

        TableName tableName = TableName.valueOf(tabName);
        if (admin.tableExists(tableName)) {
            // 表存在，删除
            deleteTable(tabName);
        }
        // 创建表
        createTable(tabName, coprocessorClass, regionCount, families);
    }

    /**
     * 删除HBase中的表
     * @param tabName
     * @throws IOException
     */
    protected void deleteTable(String tabName) throws IOException {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(tabName);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    private void createTable(String tabName, String coprocessorClass, Integer regionCount, String... families) throws IOException {
        Admin admin = getAdmin();

        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tabName));

        if (families == null || families.length == 0) {
            families = new String[]{Names.CF_INFO.getValue()};
        }

        for (String cf : families) {
            // 列描述器构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));

            // 获得列描述器
            ColumnFamilyDescriptor cfb = cdb.build();

            tdb.setColumnFamily(cfb);
        }

        if (coprocessorClass != null && "".equals(coprocessorClass)) {
            tdb.setCoprocessor(coprocessorClass);
        }

        // 获得表描述器
        TableDescriptor td = tdb.build();

        // 创建表
        if (regionCount == null || regionCount <= 0) {
            admin.createTable(td);
        } else {
            // 添加预分区
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(td, splitKeys);
        }
    }

    /**
     * 获取查询时startrow，stoprow集合
     * @param tel
     * @param start
     * @param end
     * @return
     */
    protected List<String[]> getStartStopRowkeys(String tel, String start, String end) {
        ArrayList<String[]> rowkeyss = new ArrayList<String[]>();

        String startTime = start.substring(0, 6);
        String endTime = end.substring(0, 6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DataUtil.parse(startTime, "yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DataUtil.parse(endTime, "yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()) {
            // 当前时间
            String nowTime = DataUtil.format(startCal.getTime(), "yyyyMM");

            int regionNum = genRegionNum(tel, nowTime);

            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String stopRow = startRow + "|";

            String[] rowkeys = {startRow, stopRow};
            rowkeyss.add(rowkeys);

            // 月份+1
            startCal.add(Calendar.MONTH, 1);
        }


        return rowkeyss;
    }

    /**
     * 计算分区号，关联数据放在一起
     * @param tel
     * @param date
     * @return
     */
    protected int genRegionNum(String tel, String date) {
        String userCode = tel.substring(tel.length() - 4);
        String yearMonth = date.substring(0, 6);

        // hash码
        int userCodeHash = userCode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        // crc校验
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    private byte[][] genSplitKeys(Integer regionCount){

        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];

        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }

        Collections.sort(bsList, new Bytes.ByteArrayComparator());

        bsList.toArray(bs);

        return bs;
    }

    /**
     * 增加对象，将对象保存到HBase中
     * @param obj
     */
    protected void putData(Object obj) throws Exception {
        // 反射
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
        String tabName = tableRef.value();

        Field[] fs = clazz.getDeclaredFields();
        String stringRowkey = null;
        for (Field f : fs) {
            Rowkey rowkey = f.getAnnotation(Rowkey.class);
            if (rowkey != null) {
                f.setAccessible(true);
                stringRowkey = (String) f.get(obj);
                break;
            }
        }

        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tabName));

        Put put = new Put(Bytes.toBytes(stringRowkey));

        for (Field f : fs) {
            Column column = f.getAnnotation(Column.class);
            if (column != null) {
                String family = column.family();
                String colName = column.column();
                if (colName == null || "".equals(colName)) {
                    colName = f.getName();
                }
                f.setAccessible(true);
                String value = (String) f.get(obj);

                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
            }
        }

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 增加数据
     * @param tabName
     * @param put
     */
    protected void putData(String tabName, Put put) throws IOException {
        // 获取表对象
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tabName));

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }

    /**
     * 获取连接对象
     */
    protected synchronized Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 获取连接对象
     */
    protected synchronized Connection getConnection() throws IOException {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }
}
