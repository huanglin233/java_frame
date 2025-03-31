package com.hl.bigdata.flink.es.scala

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.http.HttpHost

/**
 * @author huanglin
 * @date 2025/03/28 23:52
 */
class EsConnect {

}

object EsConnect {
  val conf: Configuration = new Configuration();
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

  def main(args: Array[String]): Unit = {
    val data = List("scala_data1", "scala_data2")
    val source: DataStream[String] = env.fromCollection(data)
    import scala.collection.mutable.MutableList
    val hosts: MutableList[HttpHost] = MutableList()
    hosts +=new HttpHost("127.0.0.1", 9200, "http")
    val index = "test"
    ElasticSearchSinkUtil_Scala.addSink(hosts, 100, 1, source,
      (element, context, indexer) => {indexer.add(ElasticSearchSinkUtil_Scala.createIndexRequest(index, element))})

    env.execute("exec es sink")
  }
}
