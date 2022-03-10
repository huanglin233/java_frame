package com.hl.bigdata.scala

import com.hl.bigdata.scala.abstarct.Animal

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
 * @author huanglin
 * @date 2022/03/08 22:24:43
 */

class Dog (val age : Int) extends Animal {
  // 定义scala类中的每一个属性，编译后，会有一个私有的字段和相应的getter、setter方法生成
  private var leg = 3;
  def shout(content : String): Unit = {
    println(content);
  }
  def currentLeg = leg;

/*  自定义Getter和Setter方法
  1) 字段属性名以“_”作为前缀，如：_leg2
  2) getter方法定义为：def leg2 = _leg2
  3) setter方法定义时方法名为属性名去掉前缀，并加上后缀，后缀是：“leg2_=”*/
  private var _leg2 = 5;
  def leg2 = _leg2
  def leg2_=(l : Int) {
    _leg2 = l;
  }
  // 对象私有字段
  private[bigdata] var var1 = null; // var1的任何类都可以被bigdata包中任何类访问
  private[Dog] var var2 = null;// 在封闭包中的任何类中可访问。
  private[this] var var3 = null;// 在封闭包中的任何类中可访问。
  def private_to(to : Dog): Unit = {
    println(to.var1)
    println(to.var2);
//    println(to.var3); 访问出错
    println(this.var3);
  }

/*  Bean属性
  将会生成四个方法：
  —— name:String
  —— name_=(newValue:String): Unit
  —— getName():String
  —– setName(newValue:String):Unit */
  @BeanProperty var name : String = _;

  // 嵌套类
  class Animal(val name : String) {
    val arr = new ArrayBuffer[Dog#Animal];
  }

  val animal = new ArrayBuffer[Animal];

  def join(name : String): Animal = {
    val n = new Animal(name);
    animal += n;
    n;
  }

  override val id: Int = 1;
  override val color: String = "red";
  override def order : Int =  {
    id;
  };
}

object Dog {

  // apply方法一般都声明在伴生类对象中，可以用来实例化伴生类的对象
  def apply(age : Int): Dog = {
    new Dog(age + 1);
  }

  private var age = 0;
  def main(args: Array[String]): Unit = {
    val dog = new Dog(17);
    dog shout "hahah"
    println(dog.currentLeg)

    dog.leg2 = (30);
    println(dog.leg2)

    dog.private_to(dog);

    dog.setName("Jack");
    println(dog.getName);

    dog.join("cg");
    dog.join("ZHQ");
    println(dog.animal(1).name);

    age += 1;
    println(age)

    val dog2 = Dog(17);
    println(dog2.age);

    println(Enum.RED);
    println(Enum.RED.id);
    println(Enum.YELLOW);
    println(Enum.YELLOW.id);
    println(Enum.GREEN);
    println(Enum.GREEN.id);
  }
}
