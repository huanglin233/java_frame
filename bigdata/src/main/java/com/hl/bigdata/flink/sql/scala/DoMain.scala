package com.hl.bigdata.flink.sql.scala

import com.hl.bigdata.flink.sql.scala.udf.{ScalarQuantityFunction, TabularValueFunction, UDAGGFunction, UDTAGGFunction}
import org.apache.flink.table.api.Expressions.{$, call, coalesce, lit}
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig, TableEnvironment}
import org.junit.Test

import java.time.Duration

/**
 * @author huanglin
 * @date 2024/11/12 21:34
 */
class DoMain {

  final val tempTableSql = "create table myTable ( id INT, name STRING, age INT, sex STRING, num INT, price BIGINT)" +
    " WITH ('connector'='jdbc', " +
    " 'url' = 'jdbc:mysql://127.0.0.1:3306/flink_test?useSSL=false', " +
    " 'table-name' = 'mytable_copy1'," +
    " 'username' = 'root'," +
    " 'password' = 'root')"

  final val sinkTableSql = "create table sinkTable ( sex STRING, udagg BIGINT, PRIMARY KEY (sex) not ENFORCED)" +
    " WITH ('connector'='jdbc', " +
    " 'url' = 'jdbc:mysql://127.0.0.1:3306/flink_test?useSSL=false', " +
    " 'table-name' = 'mytable_result'," +
    " 'username' = 'root'," +
    " 'password' = 'root')";

  val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
  val tableConfig = TableConfig.getDefault()
  tableConfig.setIdleStateRetention(Duration.ofDays(1L))
  val env = TableEnvironment.create(settings)
  env.executeSql(tempTableSql)
  env.executeSql(sinkTableSql)

  /**
   * 标量函数测试
   */
  @Test
  def scalarFunction(): Unit = {
    env.executeSql("select * from myTable;").print()

    env.from("myTable").select(call(classOf[ScalarQuantityFunction], coalesce($("name"), lit("dasdsadasd")), lit(2), lit(3)))
      .execute().print()

    env.createTemporaryFunction("ScalarQuantityFunction", classOf[ScalarQuantityFunction])

    env.from("myTable").select(call("ScalarQuantityFunction", $("name"), lit(2), lit(3)))
      .execute().print()
    env.sqlQuery("select ScalarQuantityFunction(name, 1, 2) from myTable ")
      .execute().print()
  }

  /**
   * 表值函数测试
   */
  @Test
  def tabularValueFunction(): Unit = {
    env.from("myTable")
      .joinLateral(call(classOf[TabularValueFunction], $("name")))
      .select($("id"), $("word"), $("length"))
      .execute().print()

    env.from("myTable")
      .leftOuterJoinLateral(call(classOf[TabularValueFunction], $("name")))
      .select($("id"), $("word"), $("length"))
      .execute().print()

    // 注册函数
    env.createTemporaryFunction("TabularValueFunction", classOf[TabularValueFunction])
    env.from("myTable")
      .joinLateral(call("TabularValueFunction", $("name")))
      .select($("id"), $("word"), $("length"))
      .execute().print()

    env.from("myTable")
      .leftOuterJoinLateral(call("TabularValueFunction", $("name")))
      .select($("id"), $("word"), $("length"))
      .execute().print()

    env.executeSql("SELECT id, word, length" +
      " from myTable, LATERAL TABLE(TabularValueFunction(name))")
      .print()

    env.executeSql("select id, word, length" +
      " from myTable left join lateral table(TabularValueFunction(name)) on true")
      .print()

    env.executeSql("select id, newWord, newLength" +
      " from myTable left join lateral table(TabularValueFunction(name)) as T(newWord, newLength) on true")
      .print()
  }

  /**
   * 测试自定义聚合函数
   */
  @Test
  def udfs(): Unit = {
    env.createTemporaryFunction("UDAGGFunction", classOf[UDAGGFunction])
    env.executeSql("create view groupTable as select sex, UDAGGFunction(coalesce(price, 0), coalesce(num, 0)) as udagg from myTable group by sex")
    env.executeSql("insert into sinkTable select * from groupTable").print()
  }

  /**
   * 表值聚合函数udf测试
   */
  @Test
  def udtagg(): Unit = {
    env.createTemporaryFunction("UDTAGG", classOf[UDTAGGFunction])
    val table = env.sqlQuery("select * from myTable")
    table.groupBy($("sex"))
      .flatAggregate(call("UDTAGG",$("num")).as("v", "rank"))
      .select($("sex"), $("v"), $("rank"))
      .execute().print()
  }
}
