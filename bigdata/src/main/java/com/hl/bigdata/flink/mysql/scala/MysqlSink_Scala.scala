package com.hl.bigdata.flink.mysql.scala

import org.apache.commons.dbcp.BasicDataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, Date, PreparedStatement}

/**
 * @author huanglin
 * @date 2025/04/02 21:14
 */
class MysqlSink_Scala extends RichSinkFunction[List[PageView_Scala]] {

  var statement: PreparedStatement = null
  var dataSource: BasicDataSource = null
  private var connection: Connection = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dataSource = new BasicDataSource()
    connection = getConnection(dataSource)
    val sql = "INSERT INTO page_view(user_id, event_time, page_url, id) VALUES (?, ?, ?, ?)"
    statement = connection.prepareStatement(sql)
  }

  override def invoke(value: List[PageView_Scala], context: SinkFunction.Context): Unit = {
    value.foreach(e => {
      statement.setLong(1, e.userId)
      statement.setDate(2, new Date(e.eventTime.getTime))
      statement.setString(3, e.pageUrl)
      statement.setLong(4, e.id)
      statement.addBatch()
    })

    val size = statement.executeBatch()
    print("插入数据条数：" + size.length)
  }

  override def close(): Unit = {
    super.close()
    if (connection != null) {
      connection.close()
    }

    if (statement != null) {
      statement.close()
    }
  }

  private def getConnection(dataSource :BasicDataSource): Connection = {
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/flink_test?characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true")
    dataSource.setUsername("root")
    dataSource.setPassword("root")
    dataSource.setInitialSize(10)
    dataSource.setMaxActive(50)
    dataSource.setMaxIdle(2)
    var cn: Connection = null
    try {
      cn = dataSource.getConnection()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    cn
  }
}
