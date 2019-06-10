package com.magicyoung.datapipelines.producer.io;

import com.magicyoung.datapipelines.common.bean.DataOut;

import java.io.*;

/**
 * @Author: magicyoung
 * @Date: 2019/3/3 15:27
 * @Description: 本地文件的数据输出
 */
public class LocalFileDataOut implements DataOut {
    private PrintWriter writer = null;

    public LocalFileDataOut(String path) {
        setPath(path);
    }

    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void write(Object data) throws Exception {
        write(data.toString());
    }

    /**
     * 将数据字符串生成到文件
     * @param data
     * @throws Exception
     */
    public void write(String data) throws Exception {
        writer.println(data);
        writer.flush();
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
