package com.hl.bigdata.flink.sql.scala.udf

import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @author huanglin
 * @date 2024/11/18 21:57
 */
@FunctionHint(output = new DataTypeHint("Row<word STRING, length INT>"))
class TabularValueFunction extends TableFunction[Row]{

  def eval(str : String): Unit = {
    val row = new Row(2)
    row.setField(0, str)
    row.setField(1, str.length)
    collect(row)
  }
}
