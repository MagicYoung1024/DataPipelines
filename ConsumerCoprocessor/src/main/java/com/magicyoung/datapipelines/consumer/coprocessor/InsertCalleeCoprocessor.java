package com.magicyoung.datapipelines.consumer.coprocessor;

import com.magicyoung.datapipelines.common.bean.BaseDao;
import com.magicyoung.datapipelines.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;

/**
 * @Author: magicyoung
 * @Date: 2019/3/19 8:40
 * @Description: 使用协处理器保存被叫的数据
 */
public class InsertCalleeCoprocessor implements RegionObserver {
    /**
     * 保存主叫用户数据后，由HBase自动保存被叫用户数据
     * @param c
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        // 1. 获取表
        Table table = c.getEnvironment().getConnection().getTable(TableName.valueOf(Names.TABLE.getValue()));

        String rowkey = Bytes.toString(put.getRow());

        String[] values = rowkey.split("_");

        CoprocessorDao dao = new CoprocessorDao();

        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flag = values[5];

        // 只有主叫用户保存完成后才会保存被叫用户
        if ("1".equals(flag)) {
            String calleeRowkey = dao.getRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";

            // 2. 保存数据
            Put calleePut = new Put(Bytes.toBytes(calleeRowkey));

            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));
            table.put(calleePut);

            // 3. 关闭表
            table.close();
        }
    }

    private class CoprocessorDao extends BaseDao {
        public int getRegionNum(String tel, String time) {
            return genRegionNum(tel, time);
        }
    }

}
