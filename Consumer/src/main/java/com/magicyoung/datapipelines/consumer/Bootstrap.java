package com.magicyoung.datapipelines.consumer;

import com.magicyoung.datapipelines.common.bean.Consumer;
import com.magicyoung.datapipelines.consumer.bean.CallLogConsumer;

import java.io.IOException;

/**
 * @Author: magicyoung
 * @Date: 2019/3/5 16:46
 * @Description: 启动消费者，使用kafka的消费者获取Flume采集的数据，并将数据存储到HBase
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        // 创建消费者
        Consumer consumer = new CallLogConsumer();

        // 消费数据
        consumer.consume();

        // 关闭资源
        consumer.close();
    }
}
