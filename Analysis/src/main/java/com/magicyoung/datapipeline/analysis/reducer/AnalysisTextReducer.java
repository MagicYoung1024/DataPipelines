package com.magicyoung.datapipeline.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: magicyoung
 * @Date: 2019/3/19 10:20
 * @Description: 分析数据Reducer
 */
public class AnalysisTextReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumCall = 0;
        int sumDuration = 0;
        for (Text value : values) {
            sumCall++;
            sumDuration += Integer.parseInt(value.toString());
        }
        context.write(key, new Text(sumCall + "_" + sumDuration));
    }
}
