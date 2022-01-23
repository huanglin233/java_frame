package com.hl.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/* * 
 * 自定义过滤器
 * 
 * @Author: huanglin 
 * @Date: 2022-01-23 22:48:37 
 */
public class SelfLessFilter extends FilterBase {

    private byte[]  value     = null;
    private boolean filterRow = true;

    public SelfLessFilter() {
        super();
    }

    public SelfLessFilter(byte[] value) {
        this.value = value;
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        return v;
    }

    @Override
    public void reset() throws IOException {
        this.filterRow = true;
    }

    @Override
    public boolean filterRow() throws IOException {
        return filterRow;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        if(Bytes.compareTo(value, v.getValue()) <= 0) {
            return ReturnCode.INCLUDE;
        }

        return ReturnCode.NEXT_ROW;
    }

    public byte[] toByteArray() {
        return value;
    }

    public boolean areSerializedFieldsEqual(Filter o) {
        if(o == this) {
            return true;
        }

        if(!(o instanceof SelfLessFilter)) {
            return false;
        }

        SelfLessFilter other = (SelfLessFilter) o;
        return Bytes.compareTo(this.value, other.value) == 0;
    }

    public static SelfLessFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        return new SelfLessFilter(pbBytes);
    }
}
