package com.hl.bigdata.flink.mysql.scala

import com.fasterxml.jackson.annotation.{JsonCreator, JsonFormat, JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper

import java.util.Date
import scala.beans.BeanProperty

/**
 * @author huanglin
 * @date 2025/04/02 22:39
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class PageView_Scala {
  @BeanProperty
  var userId: Long = _
  @BeanProperty
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
  var eventTime: Date = _
  @BeanProperty
  var pageUrl: String = _
  @BeanProperty
  var id: Long = _
}