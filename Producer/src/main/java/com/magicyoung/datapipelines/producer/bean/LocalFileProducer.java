package com.magicyoung.datapipelines.producer.bean;

import com.magicyoung.datapipelines.common.bean.DataIn;
import com.magicyoung.datapipelines.common.bean.DataOut;
import com.magicyoung.datapipelines.common.bean.Producer;
import com.magicyoung.datapipelines.common.utils.DataUtil;
import com.magicyoung.datapipelines.common.utils.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @Author: magicyoung
 * @Date: 2019/3/3 15:24
 * @Description: 本地数据文件的生产者
 */
public class LocalFileProducer implements Producer {
    private DataIn in;
    private DataOut out;
    private volatile boolean flag = true;

    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
        this.out = out;
    }

    public void produce() {
        try {
            // 读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);

            while (flag) {
                // 从通讯录中随机查找两个电话号码（主叫，被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true) {
                    call2Index = new Random().nextInt(contacts.size());
                    if (call1Index != call2Index) {
                        break;
                    }
                }

                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                // 生成随机的通话时间
                String startDate = "20180101000000";
                String endDate = "20190101000000";
                long startTime = DataUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endTime = DataUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

                long callTime = (long) (startTime + (endTime - startTime) * Math.random());
                String callTimeString = DataUtil.format(new Date(callTime), "yyyyMMddHHmmss");

                // 生成随机的通话时长
                String duration = NumberUtil.format(new Random().nextInt(3600), 4);

                // 生成通话记录
                CallLog log = new CallLog(call1.getTel(), call2.getTel(), callTimeString, duration);

                // 将通话记录刷写到数据文件中
                out.write(log);

                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭生产者
     * @throws IOException
     */
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }

        if (out != null) {
            out.close();
        }
    }
}
