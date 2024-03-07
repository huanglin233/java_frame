package com.hl.bigdata.scala

import java.io.IOException
import javax.inject.{Inject, Named}
import javax.validation.constraints.NotNull
import scala.collection.mutable

/**
 * @author huanglin
 * @date 2022/03/09 21:40:01
 */
//构造器注解，需要在主构造器之前，类名之后，且需要加括号，如果注解有参数，则写在注解括号里
class BigDog @Inject() (@NotNull age : Int) extends Dog(age) {

/*  1、def只能重写另一个def
  2、val只能重写另一个val或不带参数的def
  3、var只能重写另一个抽象的var*/
  override val leg2 : Int = 8;

  override def join(name: String): Animal = {
    val n = new Animal(name + "-Big");
    animal += n;
    n;
  }

  @volatile var done = false  // JVM中将成为volatile的字段
  @transient var recentLookups = new mutable.HashMap[String, String]  // 在JVM中将成为transient字段，该字段不会被序列化。
//  Scala不同，Java编译器会跟踪受检异常。如果你从Java代码中调用Scala的方法，其签名应包含那些可能被抛出的受检异常
// Java编译期需要在编译时就知道read方法可以抛IOException异常，否则Java会拒绝捕获该异常。
  @throws (classOf[IOException]) def read(filename: String) {  }
}

object BigDog {

  def main(args: Array[String]): Unit = {
    val bd = new BigDog(18);
    println(bd.age);
    bd.join("JACK");
    bd.join("TOM")
    println(bd.animal(1).name)
    println(bd.name);
    println(bd.id);
    println(bd.color);
  }
}

