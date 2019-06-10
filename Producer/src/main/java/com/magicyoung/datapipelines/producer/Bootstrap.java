package com.magicyoung.datapipelines.producer;

import com.magicyoung.datapipelines.common.bean.Producer;
import com.magicyoung.datapipelines.producer.bean.LocalFileProducer;
import com.magicyoung.datapipelines.producer.io.LocalFileDataIn;
import com.magicyoung.datapipelines.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * @Author: magicyoung
 * @Date: 2019/3/3 15:20
 * @Description: 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
//        args = new String[]{"F:\\Videos\\学习\\尚硅谷\\32_备尚硅谷大数据技术之项目：电信客服-没视频\\2.资料\\辅助文档\\contact.log", "F:\\Videos\\学习\\尚硅谷\\32_备尚硅谷大数据技术之项目：电信客服-没视频\\2.资料\\辅助文档\\call.log"};
        if (args == null || args.length < 2) {
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar xxx.jar path1 path2");
            System.exit(1);
        }
        // 构建生产者对象
        Producer producer = new LocalFileProducer();
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));

        // 生产数据
        producer.produce();

        // 关闭资源
        producer.close();
    }
}
