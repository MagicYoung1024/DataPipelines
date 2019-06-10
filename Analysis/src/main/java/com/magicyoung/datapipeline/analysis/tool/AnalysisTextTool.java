package com.magicyoung.datapipeline.analysis.tool;

import com.magicyoung.datapipeline.analysis.io.MySQLTextOutputFormat;
import com.magicyoung.datapipeline.analysis.mapper.AnalysisTextMapper;
import com.magicyoung.datapipeline.analysis.reducer.AnalysisTextReducer;
import com.magicyoung.datapipelines.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * @Author: magicyoung
 * @Date: 2019/3/19 9:52
 * @Description: 分析数据工具类
 */
public class AnalysisTextTool extends Configuration implements Tool {
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();

        job.setJarByClass(AnalysisTextTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf(Names.TABLE.getValue()),
                scan,
                AnalysisTextMapper.class,
                Text.class,
                Text.class,
                job
        );

        // reducer
        job.setReducerClass(AnalysisTextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // outputformat
        job.setOutputFormatClass(MySQLTextOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
