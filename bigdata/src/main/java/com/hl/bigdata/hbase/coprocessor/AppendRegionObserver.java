package com.hl.bigdata.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/* * 
 * 协处理器,在put数据age和name之后，自动生成no = name + age；
 * @Author: huanglin 
 * @Date: 2022-01-31 11:15:28 
 */ 
public class AppendRegionObserver extends BaseRegionObserver {
    
    private byte[] columnFamily = Bytes.toBytes("cf1");
    private byte[] qualifier1   = Bytes.toBytes("name");
    private byte[] qualifier2   = Bytes.toBytes("age");
    private byte[] qualifier3   = Bytes.toBytes("no");

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        if(put.has(columnFamily, qualifier1) || put.has(columnFamily, qualifier2)) {
            // 遍历查询结果，获取指定列的原值
            Result result = e.getEnvironment().getRegion().get(new Get(put.getRow()));
            String oldVal = "";
            for(Cell cell : result.rawCells()) {
                if(CellUtil.matchingColumn(cell, columnFamily, qualifier1)) {
                    oldVal = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }

            // 获取指定列新插入的值
            List<Cell> cells1 = put.get(columnFamily, qualifier1);
            List<Cell> cells2 = put.get(columnFamily, qualifier2);
            StringBuilder sb = new StringBuilder();
            for(Cell cell : cells1) {
                if(CellUtil.matchingColumn(cell, columnFamily, qualifier1)) {
                    sb.append("name - " + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            for(Cell cell : cells2) {
                if(CellUtil.matchingColumn(cell, columnFamily, qualifier2)) {
                    sb.append(",age - " + Bytes.toString(CellUtil.cloneValue(cell)));
                }   
            }

            // no初始值操作
            put.addColumn(columnFamily, qualifier3, Bytes.toBytes(oldVal + "; " + sb.toString()));
        }
    }
}
