package com.hl.bigdata.scala.io

import com.hl.bigdata.scala.abstarct.Animal

import java.io.{File, PrintWriter}
import scala.io.Source
import scala.reflect.runtime.universe.typeOf
import scala.util.matching.Regex

/**
 * @author huanglin
 * @date 2022/03/11 00:37:00
 */
@SerialVersionUID(1L)class FileTest {
  self =>; // 这句相当于给this起了一个别名为self：
  val x=2
  def foo = println(self.x + this.x);
}

object FileTest extends App {
  val file = Source.fromFile("/home/huanglin/cv_debug.log", "UTF-8");
  val lines = file.getLines();
  for(line <- lines) {
    println(line);
  }

  println(file.getLines().toArray);
  println(file.mkString.split(","));

  file.close();

  val webFile = Source.fromURL("https://huanglin.online");
  webFile.foreach(print);

  val writer = new PrintWriter(new File("/home/huanglin/blog.txt"));
  webFile.foreach(x => writer.println(x));
  for (i <- 1 to 100)
  writer.println(i)
  writer.close();

  webFile.close();

  println();
  val pattern1 = new Regex("(S|s)cala")
  val pattern2 = "(S|s)cala".r
  val str = "Scala is scalable and cool"
  println((pattern2 findAllIn str).mkString(","))

  println(typeOf[FileTest])
  println(classOf[FileTest]);

//  可以通过type关键字来创建一个简单的别名，类型别名必须被嵌套在类或者对象中，不能出现在scala文件的顶层：
  import scala.collection.mutable._
  type Index = HashMap[String, (Int, Int)]
//  中置类型是一个带有两个类型参数的类型，以中置语法表示，比如可以将Map[String, Int]表示为：
  val scores: String Map Int = Map("Fred" -> 42)

//  运行时类成员的访问
  val ru = scala.reflect.runtime.universe;
  val mirror = ru.runtimeMirror(getClass.getClassLoader);
  //得到FileTest类的Type对象后，得到type的特征值并转为ClassSymbol对象
  val classPerson = ru.typeOf[FileTest].typeSymbol.asClass;
  //用Mirrors去reflect对应的类,返回一个Mirrors的实例,而该Mirrors装载着对应类的信息
  val classMirror = mirror.reflectClass(classPerson);

  //得到构造器Method
  val constructor = ru.typeOf[FileTest].decl(ru.termNames.CONSTRUCTOR).asMethod;
  //得到MethodMirror
  val methodMirror = classMirror.reflectConstructor(constructor);
  //实例化该对象
  val p = methodMirror();
  println(p);

  //反射方法并调用
  val instanceMirror = mirror.reflect(p);
  //得到Method的Mirror
  val myPrintMethod = ru.typeOf[FileTest].decl(ru.TermName("foo")).asMethod;
  //通过Method的Mirror索取方法
  val myPrint = instanceMirror.reflectMethod(myPrintMethod);
  //运行myPrint方法
  myPrint();

  //得到属性Field的Mirror
  val xField = ru.typeOf[FileTest].decl(ru.TermName("x")).asTerm;
  val x = instanceMirror.reflectField(xField);
  println(x.get);
}
