package com.magicyoung.datapipeline.analysis;

import com.magicyoung.datapipeline.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: magicyoung
 * @Date: 2019/3/19 9:49
 * @Description: 分析数据
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new AnalysisTextTool(), args);
    }
}
