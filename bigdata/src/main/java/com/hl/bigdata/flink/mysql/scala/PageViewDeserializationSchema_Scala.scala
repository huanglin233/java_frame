package com.hl.bigdata.flink.mysql.scala

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * @author huanglin
 * @date 2025/04/02 22:46
 */
class PageViewDeserializationSchema_Scala extends DeserializationSchema[PageView_Scala]{

  val objectMapper: ObjectMapper = new ObjectMapper()

  override def deserialize(bytes: Array[Byte]): PageView_Scala = {
    objectMapper.readValue(bytes, classOf[PageView_Scala])
  }

  override def isEndOfStream(t: PageView_Scala): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[PageView_Scala] = {
    TypeInformation.of(classOf[PageView_Scala])
  }
}
