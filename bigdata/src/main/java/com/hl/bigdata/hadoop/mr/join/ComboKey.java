package com.hl.bigdata.hadoop.mr.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 组合key
 * 
 * @author huanglin
 * @date 2021/08/08 15/16/27
 */
public class ComboKey implements WritableComparable<ComboKey>{

    // id
    private int customerId;
    // 标记位 0-customer/1-order
    private int flag;

    public ComboKey() {}
    public ComboKey(int customerId, int flag) {
        this.customerId = customerId;
        this.flag       = flag;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(customerId);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.customerId = in.readInt();
        this.flag       = in.readInt();
    }

    @Override
    public int compareTo(ComboKey o) {
        int id0   = o.getCustomerId();
        int flag0 = o.getFlag();
        if(customerId != id0) {
            return customerId - id0;
        } else {
            return flag - flag0;
        }
    }

    @Override
    public String toString() {
        return "(" + customerId + "," + flag + ")";
    }
}