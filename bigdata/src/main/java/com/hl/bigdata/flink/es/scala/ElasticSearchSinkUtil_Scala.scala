package com.hl.bigdata.flink.es.scala

import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, ElasticsearchEmitter}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util
import scala.collection.mutable

/**
 * @author huanglin
 * @date 2025/03/28 21:05
 */
object ElasticSearchSinkUtil_Scala {

  def addSink[T](hosts: mutable.MutableList[HttpHost], bulkFlushMaxActions: Int, parallelism: Int, data: DataStream[T],
                 sendEmitter: ElasticsearchEmitter[String]) = {
    val build: Elasticsearch7SinkBuilder[T] = new Elasticsearch7SinkBuilder()
    build.setHosts(hosts: _*)
      .setBulkFlushMaxActions(bulkFlushMaxActions)
      .setEmitter(sendEmitter)

    data.sinkTo(build.build()).setParallelism(parallelism);
  }

  def createIndexRequest(index: String, element: String): IndexRequest = {
    val json: util.HashMap[String, String] = new util.HashMap;
    json.put("data", element);

    Requests.indexRequest()
      .index(index)
      .id(element)
      .source(json)
  }
}
