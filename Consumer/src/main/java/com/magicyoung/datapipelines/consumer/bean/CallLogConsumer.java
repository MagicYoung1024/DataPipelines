package com.magicyoung.datapipelines.consumer.bean;

import com.magicyoung.datapipelines.common.bean.Consumer;
import com.magicyoung.datapipelines.common.constant.Names;
import com.magicyoung.datapipelines.consumer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: magicyoung
 * @Date: 2019/3/5 16:49
 * @Description: 通话日志的消费者对象
 */
public class CallLogConsumer implements Consumer {
    /**
     * 消费数据
     */
    public void consume() {
        try {
            // 创建配置对象
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));

            // 获取Flume采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

            // 关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

            // HBase数据访问对象
            HBaseDao dao = new HBaseDao();
            dao.init();

            // 消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());
                    // dao.insertData(consumerRecord.value());
                    CallLog log = new CallLog(consumerRecord.value());
                    dao.insertData(log);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    public void close() throws IOException {

    }
}
