package com.hl.bigdata.flink.sql.java.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * flink udf 标量函数
 *  -- 把0到多个标量值映射成1个标量值
 *
 * @author huanglin
 * @date 2024/11/12 21:24
 */
public class ScalarQuantityFunction extends ScalarFunction {

    /**
     * 接受任务类型得输入,返回int类型输出
     * @param val 输入值
     * @return
     */
    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object val) {
        return val != null ? val.hashCode() : -1;
    }
}
