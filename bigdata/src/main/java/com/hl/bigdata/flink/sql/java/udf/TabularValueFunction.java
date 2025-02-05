package com.hl.bigdata.flink.sql.java.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * udf 表值函数
 * 把0到多个标量值映射成多行数据
 *
 * @author huanglin
 * @date 2024/11/13 14:13
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class TabularValueFunction extends TableFunction<Row> {

    public void eval(String str) {
        if(str == null || str.isEmpty()) {
            return;
        }
        for(String  s : str.split(";")) {
            collect(Row.of(s, s.length()));
        }
    }
}
