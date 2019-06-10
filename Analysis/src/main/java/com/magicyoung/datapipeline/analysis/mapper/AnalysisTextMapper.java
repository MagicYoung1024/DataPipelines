package com.magicyoung.datapipeline.analysis.mapper;


import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Author: magicyoung
 * @Date: 2019/3/19 10:08
 * @Description: 分析数据的Mapper
 */
public class AnalysisTextMapper extends TableMapper<Text, Text> {
    Text v = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowkey = Bytes.toString(key.get());

        String[] values = rowkey.split("_");

        String call1 = values[1];
        String call2 = values[3];
        String callTime = values[2];
        String duration = values[4];

        String year = callTime.substring(0, 4);
        String month = callTime.substring(4, 6);
        String day = callTime.substring(6, 8);

        v.set(duration);
        // 主叫用户
        // 日
        context.write(new Text(call1 + "_" + day), v);
        // 月
        context.write(new Text(call1 + "_" + month), v);
        // 年
        context.write(new Text(call1 + "_" + year), v);

        // 被叫用户
        // 日
        context.write(new Text(call2 + "_" + day), v);
        // 月
        context.write(new Text(call2 + "_" + month), v);
        // 年
        context.write(new Text(call2 + "_" + year), v);

    }
}