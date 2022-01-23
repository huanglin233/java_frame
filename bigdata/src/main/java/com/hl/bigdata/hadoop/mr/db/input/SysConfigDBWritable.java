package com.hl.bigdata.hadoop.mr.db.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 * sys_config beanObj
 * 
 * @author huanglin
 * @date 2021/08/07 12/04/06
 */
public class SysConfigDBWritable implements DBWritable, Writable {

    private String variable;
    private String value;
    private Date   setTime;
    private String setBy;

    public String getVariable() {
        return variable;
    }

    public void setVariable(String variable) {
        this.variable = variable;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Date getSetTime() {
        return setTime;
    }

    public void setSetTime(Date setTime) {
        this.setTime = setTime;
    }

    public String getSetBy() {
        return setBy;
    }

    public void setSetBy(String setBy) {
        this.setBy = setBy;
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.variable = resultSet.getString("variable");
        this.value    = resultSet.getString("value");
        this.setBy    = resultSet.getString("setBy");
    }

    /** 
     * hadoop 串行化
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.variable);
        out.writeUTF(this.value);
        out.writeUTF(this.setTime.toString());
        out.writeUTF(this.setBy);
    }

    /**
     * hadoop 串行化
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.variable = in.readUTF();
        this.value    = in.readUTF();
        this.setBy    = in.readUTF();
    }
}