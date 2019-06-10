package com.magicyoung.datapipelines.common.bean;

/**
 * @Author: magicyoung
 * @Date: 2019/3/3 15:13
 * @Description: 数据对象
 */
public abstract class Data implements Value {
    public String content;

    public void setValue(Object value) {
        content = (String)value;
    }

    public String getValue() {
        return content;
    }
}