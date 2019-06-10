package com.magicyoung.datapipelines.consumer.dao;

import com.magicyoung.datapipelines.common.bean.BaseDao;
import com.magicyoung.datapipelines.common.constant.Names;
import com.magicyoung.datapipelines.common.constant.ValueConstant;
import com.magicyoung.datapipelines.consumer.bean.CallLog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: magicyoung
 * @Date: 2019/3/5 18:03
 * @Description: HBase数据访问对象
 */
public class HBaseDao extends BaseDao {
    /**
     * 初始化
     */
    public void init() throws IOException {
        start();

        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(), "com.magicyoung.datapipelines.consumer.coprocessor.InsertCalleeCoprocessor", ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue(), Names.CF_CALLEE.getValue());
        
        end();
    }

    /**
     * 插入对象
     * @param log
     */
    public void insertData(CallLog log) throws Exception {
        String rowkey = genRegionNum(log.getCall1(), log.getCalltime()) + "_" + log.getCall1() + "_" + log.getCalltime() + "_" + log.getCall2() + "_" + log.getDuration();
        log.setRowkey(rowkey);
        putData(log);
    }

    /**
     * 插入数据，将通话日志保存到HBase表中
     * @param value
     */
    public void insertData(String value) throws IOException {
        // 1. 获取通话日志数据
        String[] values = value.split("\t");

        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        // 2. 创建数据对象，主叫用户
        String rowkey = genRegionNum(call1, calltime) + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";
        Put put = new Put(Bytes.toBytes(rowkey));

        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());
        put.addColumn(family, Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(family, Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(family, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        put.addColumn(family, Bytes.toBytes("flag"), Bytes.toBytes("1"));

        // 被叫用户
/*        String calleeRowkey = genRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";
        Put calleePut = new Put(Bytes.toBytes(rowkey));

        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));*/

        // 3. 保存数据
        putData(Names.TABLE.getValue(), put);
//        putData(Names.TABLE.getValue(), calleePut);
    }
}
