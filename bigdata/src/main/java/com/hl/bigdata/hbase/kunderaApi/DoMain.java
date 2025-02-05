package com.hl.bigdata.hbase.kunderaApi;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/* *
 * Kundera操作hbase实体类
 * @Author: huanglin
 * @Date: 2022-02-03 20:42:46
 */

@Entity
@Table(name = "cf1")
public class DoMain implements Serializable {

    @Id
    private String id;

    @Column
    private String no;

    @Column
    private String name;

    @Column
    private int age;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
