package com.magicyoung.datapipelines.common.constant;

import com.magicyoung.datapipelines.common.bean.Value;

/**
 * 名称常量枚举类
 */
public enum Names implements Value {
    NAMESPACE("ct"),
    TABLE("ct:calllog"),
    CF_CALLER("caller"),
    CF_CALLEE("callee"),
    CF_INFO("info"),
    TOPIC("ct");

    private String name;

    private Names(String name) {
        this.name = name;
    }

    public void setValue(Object value) {
        this.name = (String) value;
    }

    public String getValue() {
        return name;
    }
}
