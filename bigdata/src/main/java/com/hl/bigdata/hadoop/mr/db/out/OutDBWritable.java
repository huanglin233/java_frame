package com.hl.bigdata.hadoop.mr.db.out;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * out input dbwritable
 * 
 * @author huanglin
 * @date 2021/08/07 17/50/25
 */
public class OutDBWritable implements DBWritable, Writable{

    private Long    id;
    private Integer version;
    private Integer resource;
    private String  description;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Integer getResource() {
        return resource;
    }

    public void setResource(Integer resource) {
        this.resource = resource;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeInt(version);
        out.writeInt(resource);
        out.writeUTF(description);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id          = in.readLong();
        this.version     = in.readInt();
        this.resource    = in.readInt();
        this.description = in.readUTF();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, id);
        statement.setInt(2, version);
        statement.setInt(3, resource);
        statement.setString(4, description);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {}
}