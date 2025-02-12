package com.hl.bigdata.flink.custom.scala

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{ProcessingTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.util

/**
 * @author huanglin
 * @date 2025/02/13 00:01
 */
class CustomWindowAssigner(windowSize: Long) extends WindowAssigner[Object, TimeWindow] {

  override def assignWindows(t: Object, timestamp: Long, windowAssignerContext: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = {
    val start = timestamp - (timestamp % windowSize)
    val end = start + windowSize
    val result = new util.ArrayList[TimeWindow](1)
    result.add(new TimeWindow(start, end))
    result
  }

  override def getDefaultTrigger(streamExecutionEnvironment: StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    ProcessingTimeTrigger.create
  }

  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }

  override def isEventTime: Boolean = false
}

object CustomWindowAssigner {
  private val DEFAULT_WINDOW_SIZE = 5000L // 5 seconds default

  def apply(): CustomWindowAssigner = new CustomWindowAssigner(DEFAULT_WINDOW_SIZE)

  def apply(windowSize: Long): CustomWindowAssigner = new CustomWindowAssigner(windowSize)
}
