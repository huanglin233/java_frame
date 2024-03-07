package com.hl.bigdata.spark.scala.customObj

import org.apache.spark.util.AccumulatorV2

import java.util

/**
 * 自定义累加器
 *
 * @author huanglin
 * @date 2022/04/12 22:56:51
 */
class CustomAccumulator extends AccumulatorV2[String, java.util.Set[String]]{

  // 定义一个累加器放入内存结构，用于保存带有字母的字符串
  private val _customArray : java.util.Set[String] = new util.HashSet[String]();

  // 重写检测累加器内部数据机构是否为空
  override def isZero : Boolean = {
    _customArray.isEmpty;
  }

  // 让spark框架能够调用copy函数产生一个新的系统类.即；累加器实例
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val acc = new CustomAccumulator();
    _customArray.synchronized{
      acc._customArray.addAll(_customArray);
    }

    acc;
  }

  // 重置累加器的数据结构
  override def reset(): Unit = {
    _customArray.clear();
  }

  // 提供转换或者行动操作中添加累加器值的方法
  override def add(v: String): Unit = {
    _customArray.add(v);
  }

  // 多分区的累计器的值进行合并
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    // 通过类型监测将这个累加器值加入到_customArray机构中
    other match {
      case  m : CustomAccumulator => _customArray.addAll(m.value);
    }
  }

  // 获取累加值
  override def value: java.util.Set[String]  = {
    java.util.Collections.unmodifiableSet(_customArray);
  }
}
