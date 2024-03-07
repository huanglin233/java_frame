package com.hl.bigdata.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.parquet.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author huanglin
 * @date 2023/02/18 16:06
 */
public class SaltRangeTableInputFormat extends TableInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String tableName = conf.get(TableInputFormat.INPUT_TABLE);
        if(Strings.isNullOrEmpty(tableName)) {
            throw new IOException("tableName must be provided");
        }

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName table = TableName.valueOf(tableName);
        RegionLocator regionLocator = connection.getRegionLocator(table);

        String scanStart = conf.get(TableInputFormat.SCAN_ROW_START);
        String scanStop = conf.get(TableInputFormat.SCAN_ROW_STOP);

        Pair<byte[][], byte[][]> keys = regionLocator.getStartEndKeys();
        if(keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
            throw new RuntimeException("At least one region is expected");
        }
        List<InputSplit> splits = new ArrayList<>(keys.getFirst().length);
        for(int i = 0; i < keys.getFirst().length; i++) {
            String regionLocation = getTableRegionLocation(regionLocator, keys.getFirst()[i]);
            String regionSalt = null;
            if(keys.getFirst()[i].length > 0) {
                regionSalt = Bytes.toString(keys.getFirst()[i]).split("-")[0];
            }

            byte[] startRowKey = Bytes.toBytes(regionSalt + "-" + scanStart);
            byte[] endRowKey = Bytes.toBytes(regionSalt + "-" + scanStop);

            InputSplit split = new TableSplit(TableName.valueOf(tableName), startRowKey, endRowKey, regionLocation);
            splits.add(split);
        }


        return splits;
    }

    private String getTableRegionLocation(RegionLocator regionLocator, byte[] rowKey) throws IOException {
        return regionLocator.getRegionLocation(rowKey).getHostname();
    }
}
