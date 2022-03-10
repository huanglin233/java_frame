package com.hl.bigdata.scala

import com.hl.bigdata.scala.`trait`.{AnimalLogger, BigDogLogger, DogLogger, FileLogger, Logger}
import com.hl.bigdata.scala.abstarct.Animal

import java.util.Date

/**
 * @author huanglin
 * @date 2022/03/09 21:54:51
 */
class ConsoleLogger extends Animal with Logger {
  override def log(msg: String): Unit = {
    println(msg);
  }

  override val id: Int = 3;
  override val color: String = "red";

  override def order: Int = {
    return this.id + 3;
  }

  override def abstract_log(msg: String): Unit = {}

  val name : String = "dog";
}

object ConsoleLogger {
  def main(args: Array[String]): Unit = {
    val logger = new ConsoleLogger();
    logger.log("log1");
    logger.log2("log2");


//    继承多个相同父特质的类，会从右到左依次调用特质的方法
    val log1 = new ConsoleLogger with DogLogger with BigDogLogger;
    val log2 = new ConsoleLogger with BigDogLogger with DogLogger;
    println(log1.log(log1.color));
    println(log2.log(log2.color));
//    Thu Mar 10 21:15:29 CST 2022 Dog  Thu Mar 10 21:15:29 CST 2022 BigDog red
//    ()
//    Thu Mar 10 21:15:29 CST 2022 BigDog  Thu Mar 10 21:15:29 CST 2022 Dog red
//    ()

    val log3 = new ConsoleLogger with DogLogger with BigDogLogger;
    println(log3.abstract_log(log3.color));
//    Thu Mar 10 21:21:36 CST 2022 Dog  Thu Mar 10 21:21:36 CST 2022 BigDog red
//    ()

//    特质中可以定义具体字段，如果初始化了就是具体字段，如果不初始化就是抽象字段。
//    混入该特质的类就具有了该字段，字段不是继承，而是简单的加入类。是自己的字段。
    val log4 = new ConsoleLogger with DogLogger with BigDogLogger;
    println(log4.trait_name0 + " " + log4.trait_name1);

    val fileLogger = new ConsoleLogger with FileLogger {
      val date = new Date();
      override val fileName: String = date.getTime().toString();
    }
    fileLogger.log("big red dog");
  }
}
