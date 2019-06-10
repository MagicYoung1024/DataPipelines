package com.magicyoung.datapipelines.producer.bean;

import com.magicyoung.datapipelines.common.bean.Data;

/**
 * @Author: magicyoung
 * @Date: 2019/3/3 15:44
 * @Description: 联系人对象
 */
public class Contact extends Data {
    private String tel;
    private String name;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setValue(Object value) {
        content = (String)value;
        String[] values = content.split(" ");
        setName(values[1]);
        setTel(values[0]);
    }

    @Override
    public String toString() {
        return "Contact[" + tel + ", " + name + "]";
    }
}
