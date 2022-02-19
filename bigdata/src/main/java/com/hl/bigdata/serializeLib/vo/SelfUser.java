package com.hl.bigdata.serializeLib.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

/* *
 * java序列化对象 
 * @Author: huanglin 
 * @Date: 2022-02-19 19:01:06 
 */ 
public class SelfUser implements Writable, Serializable {

    private static final long serialVersionUID = 1L;

    private String  name;
    private Integer favoriteNumber;
    private String  favoriteColor;

    public SelfUser(String name, Integer favoriteNumber, String favoriteColor) {
        super();
        this.name           = name;
        this.favoriteNumber = favoriteNumber;
        this.favoriteColor  = favoriteColor;
    }

    public SelfUser(){}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(Integer favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(favoriteNumber);
        out.writeUTF(favoriteColor);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name           = in.readUTF();
        this.favoriteNumber = in.readInt();
        this.favoriteColor  = in.readUTF();
    }
}