package com.hl.bigdata.flink.sql.scala.udf

import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.functions.ScalarFunction


/**
 * flink udf 标量函数
 * -- 把0到多个标量值映射成1个标量值
 *
 * @author huanglin
 * @date 2024/11/12 21:20
 */
class ScalarQuantityFunction extends ScalarFunction {

  def eval(s: String, begin: Integer, end: Integer): String = {
    s.substring(begin, end)
  }
}
