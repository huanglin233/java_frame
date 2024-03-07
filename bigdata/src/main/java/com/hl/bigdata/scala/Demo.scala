package com.hl.bigdata.scala

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author huanglin
 * @date 2022/03/02 22:23:8
 */
class Demo {
  def add(a : Int, b : Int) : Int = a + b;
  // scala中常用的循环方式
  def cycle() : Unit = {
    var i = 0;
    while(i < 10) {
      println(i)
      i+=1;
    }
    println("------------");
    for(i <- 1 to 20) {
      println(i)
    }
    println("------------");
    for(i <- 1 until 30) {
      println(i)
    }
    println("------------");
    for{
      i <- 1 to 3
      j = 4 -i
    } {
      print(j)
    };
  }
  // yield
  def yieldTest(): Unit = {
    var x = for(i <- 1 to 10) yield i % 3;
    println(x);
  }
  // 函数返回值多种类型可能，省略Unit
  def def1(msg : String) = {
    if(msg.equals("hello")) {
      msg + " too";
    } else {
      1;
    }
  }
  // 带默认值的参数
  def def2(msg : String, len : Int = 3): Unit = {
    println(msg + " : " +3);
  }
  // 可变变长参数,  所有参数会转化为一个seq序列
  def def3(i : Int*): Unit = {
    var j : Int = 0;
    for(x <- i) {
      j += x;
    }
    println(j);
  }
  // 懒值
  def lazyTest1() : String = {
    println("lazy方式执行");
    "lazy执行结果"
  }
  def lazyTest2() : Unit =  {
    lazy val msg = lazyTest1();
    println("----");
    println(msg);

  }
  // 异常
  def throwable(x : Int, y : Int) : Float = {
    if(y == 0)
      throw new Exception("0不能作为除数");
    else
      x / y;
  }
  def catch_ex() : Unit = {
    try {
      throwable(0, 0);
    } catch {
      case ex : Exception => {
        print("捕获异常： " + ex.toString);
      }
    }
  }
  // 数组
  def array_() : Unit = {
    // 定长数组
    val arr1 = new Array[Int](10);
    arr1(1) = 7;
    val arr2 = Array(1, 2);
    println(arr1(1))
    // 变长数组
    val arr3 = ArrayBuffer[Int]();
    arr3.append(8);
    arr3.append(9);
    arr3(1) = 8;
    println(arr3.toString());
    // 定长数组和变长数组的转换
    arr1.toBuffer;
    arr2.toArray;
    // 多位数组
    val arr4 = Array.ofDim[Double](3,4);
    arr4(1)(1) = 33.33;
    println(arr4(1)(1));
    // Scala数组转Java数组, scala为2.13.2版本
    val arr5 = ArrayBuffer("1", "2", "3")
//    import scala.jdk.CollectionConverters.BufferHasAsJava;
//    val javaArr : java.util.List[String] = arr5.asJava;
//    println(javaArr)
    // java数组转scala, scala为2.13.2版本
//    import scala.jdk.CollectionConverters.CollectionHasAsScala
//    val scalaArr : mutable.Buffer[String] = javaArr.asScala.toBuffer;
//    println(scalaArr.toString());
    // 数组循环
//    for(x <- scalaArr) {
//      print(x + " ");
//    }
  }
  // 元组tuple
  def tuple_() : Unit = {
    val tuple1 = (1, 2, 3, "hello");
    println(tuple1);
    // 值访问
    val value1 = tuple1._1;
    println(value1);
    // 值遍历
    for(t <- tuple1.productIterator) {
      print(t + " ");
    }
    tuple1.productIterator.foreach(t => print(t + " "));
    tuple1.productIterator.foreach(print(_));
  }
  // 列表 list
  def list_() : Unit = {
    val list1 = List(1, "s");
    println(list1.toString());
    // 访问值
    val value1 = list1(1);
    println(value1);
    // 追加元素
    val list2 = list1 :+ 99;
    println(list2);
    val list3 = 100 +: list2;
    println(list3);
    val list4 = 1 :: 2 :: 3 :: list3;
    println(list4);
    val list5 = 5 :: list4 :: Nil;
    println(list5);
    val list6 = 1 :: 2 :: Nil;
    println(list6);
  }
  // 队列queue
  def queue_() : Unit = {
    val q1 = new mutable.Queue[Int];
    println(q1);
    // 队列追加元素
    q1 += 1;
    println(q1);
    println(q1(0));
    // 追加list
    q1 ++= List(2, 3, 4);
    println(q1);
    // 按照传入队列的顺序删除元素
    q1.dequeue();
    println(q1);
    // 塞入数据
    q1.enqueue(8, 9 , 10);
    println(q1);
    // 返回队列第一个元素
    println(q1.head);
    // 返回队列最后一个元素
    println(q1.last);
    // 返回除第一个元素之外的元素
    println(q1.tail);
  }
  // map
  def map_() : Unit = {
    // 不可变map
    val map1 = Map("jack" -> 10, "tom" -> "13");
    println(map1);
    // 可变map
    var map2 = scala.collection.mutable.Map("jack" -> 10, "tom" -> "13");
    println(map2);
    println(map2.get("jack").get);
    // 空的map
    val map3 = new mutable.HashMap[String, Int];
    println(map3);
    // 对偶原组
    val map4 = Map(("jack", 18), ("tom", 20));
    println(map4);
    println(map4("jack"));
    // 更新值
    map2("jack") = 12;
    println(map2);
    map2 += ("jack" -> 13);
    println(map2);
    map2 -= ("jack");
    println(map2);
    val map5 = map2 + ("jack" -> 11, "merry" -> 13);
    println(map5);
    // 循环
    for((k, v) <- map5) {
      println(k + " -> " + v);
    }
    for(k <- map5.keys) {
      println(k);
    }
    for(v <- map5.values) {
      println(v);
    }
    for(m <- map5) {
      println(m);
    }
  }
  // set
  def set_(): Unit = {
    val set1 = Set(1, 2, 3);
    println(set1);
    val set2 = mutable.Set(1, 2,3);
    println(set2);
    // 可变集合添加元素
    set2.add(4);
    set2 += 5;
    println(set2);
    // 这个方法是返回一个新的Set集合，原集合不进行元素添加
    val set3 = set2.+(6);
    println(set2);
    println(set3);
    // 可变集合删除出元素
    set3 -= 1;
    set3.remove(2);
    println(set3);
    // 集合遍历
    for(x <- set3) {
      print(x + " ");
    }
  }
  // 集合一些方法操作
  def coll_fun(): Unit = {
    val names = List("Jack", "Tom", "Merry");
    println(names.map(_.toUpperCase()));
    // flatmap：flat即压扁，压平，扁平化，效果就是将集合中的每个元素的子元素映射到某个函数并返回新的集合
    val names2 = List("Jack", "Tom", "Merry");
    println(names2.flatMap(_.toUpperCase()));
    // 折叠，化简：将二元函数引用于集合中的函数
    val list = List(1, 2, 3, 4, 5);
    val l1 = list.reduceLeft( _ - _);
    println(l1);
    val l2 = list.reduceRight(_ - _);
    println(l2);
    // 折叠，化简：fold,  fold函数将上一步返回的值作为函数的第一个参数继续传递参与运算，直到list中的所有元素被遍历
    val list1 = List(1, 9 , 2, 8);
    val l3 = list1.fold(5)((sum, y) => {
      sum + y;
    });
    println(l3);
    val l4 = list1.foldLeft(100)(_ - _);
    println(l4);
    // 统计一句话中，各个文字出现的次数
    val sentence = "采薇采薇，薇亦作止。 曰归曰归，岁亦莫止。";
    val l5 = (Map[Char, Int]() /: sentence)((m, c) => m + ( c -> (m.getOrElse(c, 0) + 1)));
    val l6 = sentence.foldLeft(Map[Char, Int]()) ((m, c) => {
      m + (c -> (m.getOrElse(c, 0) + 1));
    })
    println(l5);
    println(l6);
    // 拉链操作，将两个集合转化为tuple,长度以最短的集合为准
    val list2 = List("jack", " tom", "merry");
    val list3 = List(18, 16);
    println(list2.zip(list3));
    // 迭代器
    val list4 = List("a", "b", "c");
    val it = list4.iterator;
    while(it.hasNext) {
      println(it.next())
    }
    // Stream流,末尾元素遵循lazy规则
    // 使用#::得到一个stream
    def numsFrom(n : BigInt) : Stream[BigInt] = n #:: numsFrom(n + 1);
    val ternOrMore = numsFrom(10);
    println(ternOrMore);
    println(ternOrMore.tail);
    println(ternOrMore);
    println(ternOrMore.map(x => x * x));
    // 视图View,该方法产出一个其方法总是被懒执行的集合。但是view不会缓存数据，每次都要重新计算
    val viewSquares = (1 to 100000).view.map(x => {
      x.toLong * x.toLong
    }).filter(x => {
      x.toString == x.toString.reverse;
    })
    for(view <- viewSquares) {
      print(view + ", ");
    }
    // 并行集合,Scala 2.13之后，并行集合模块变成了外部库，直接像2.12那样写如下的代码，IDE会报“Cannot resolve symbol par”
//    import scala.collection.parallel.CollectionConverters._
    println();
    (1 to 5).foreach(print(_));
    println();
//    (1 to 5).par.foreach(print(_));
    println();
    val res1 = (0 to 10000).map(_ => Thread.currentThread().getName).distinct
//    val res2 = (0 to 10000).par.map(_ => Thread.currentThread().getName).distinct
    println(res1);
//    println(res2);
  }
  // 模式匹配
  def match_(): Unit ={
    var res = 0;
    val op : Char = '-';
    op match {
      case '+' => res = 1;
      case '-' => res = - 1;
      case _ => res = 0;
    }
    println(res);

    // 匹配中的守护
    for(ch <- "+-3!") {
      var sign = 0;
      var digit = 0;

      ch match {
        case '+' => sign = 1;
        case '-' => sign = -1;
        case _ if ch.toString.equals("3") => digit = 3;
        case _ => sign = 0;
      }
      println(ch + " " + sign + " " + digit);
    }

    // 模式中的变量, 如果在case关键字后跟变量名，那么match前表达式的值会赋给那个变量
    val str = "+-3!";
    for(i <- str.indices) {
      var sign = 0;
      var digit = 0;

      str(i) match {
        case '+' => sign = 1;
        case '-' => sign = -1;
        case ch if Character.isDigit(ch) => digit = Character.digit(ch, 10);
        case _ =>
      }
      println(str(i) + " " + sign + " " + digit);
    }

    // 类型模式
    val a = 8;
    val obj = if(a == 1) 1;
    else if(a == 2) "2";
    else if(a == 3) BigInt(3);
    else if(a == 4) Map("aa" -> 1);
    else if(a == 5) Map(1 -> "aa");
    else if(a == 6) Array(1, 2, 3);
    else if(a == 7) Array("aa", 1);
    else if(a == 8) Array("aa");
    var r1 = obj match {
      case x : Int => x;
      case x : String => x.toInt;
      case _ : BigInt => Int.MaxValue
//      case m : Map[String, Int] => "Map[String, Int]类型的Map集合";
      case m : Map[_, _] => "Map集合";
      case a : Array[Int] => "Array[Int]类型的Int集合";
      case a : Array[String] => "Array[String]类型的String集合";
      case a : Array[_] => "Array集合";
      case _ => 0;
    }
    println(r1);

    // 匹配数组\列表\元组
    for(arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0), Array(1, 1, 0, 1))) {
      val res = arr match {
        case  Array(0) => "0";
        case Array(x, y) => x + " " + y;
        case Array(x, y , z) => x + " " + y + " " + z;
        case Array(0, _*) => "0...";
        case _ => "no do something"
      }
      println(res);
    }
    for(arr <- List(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0))) {
      val res = arr match {
        case 0 :: Nil => '0';
        case x :: y :: Nil => x + " " + y;
        case 0 :: tail => "0...";
        case _ => "no do something";
      }
      println(res);
    }
    for(pair <- Array((0, 1), (1, 0), (1, 1))) {
      val res = pair match {
        case (0, _) => "0...";
        case (y, 0) => y + " 0";
        case _ => "no do something";
      }
      println(res);
    }

    // 提取器
    /**
     * —— 调用unapply，传入number
     * —— 接收返回值并判断返回值是None，还是Some
     * —— 如果是Some，则将其解开，并将其中的值赋值给n（就是case Square(n)中的n）
     */
    object Square {
      def unapply(z : Double) : Option[Double] = Some(math.sqrt(z));
    }
    val number : Double = 36.0;
    number match {
      case Square(n) => println(s"square root of $number is $n");
      case _ => println("no do something")
    }

    /**
     * —— 调用unapplySeq，传入namesString
     * —— 接收返回值并判断返回值是None，还是Some
     * —— 如果是Some，则将其解开
     * —— 判断解开之后得到的sequence中的元素的个数是否是和接受匹配的参数一致
     * —— 如果是一致，则把这些元素分别取出按顺序赋值给响应参数
     */
    object Names {
      def unapplySeq(str : String) : Option[Seq[String]] = {
        if(str.contains(",")) Some(str.split(","));
        else None
      }
    }
    val nameString = "Jack,Tome,Merry,J";
    nameString match {
      case Names(first, second, third, four) => {
        println("the string contains four people's names");
        println(s"$first $second $third $four");
      }
      case _ => println("nothing matched");
    }

    // 变量声明中模式, match中每一个case都可以单独提取出来，意思是一样的
    val (x, y) = (1, 2);
    println(x + " "+ y);
    val (q, r) = BigInt(10) /% 3;
    println(q + " " + r);
    val arr = Array(1, 7, 2, 9);
    val Array(first, second, _*) = arr;
    println(first, second);

    // for 表达式中的模式
    import scala.collection.JavaConverters._
    for((k, v) <- System.getProperties.asScala) {
      println(k + " -> " + v);
    }
    println("------------------------")
    for((k, "") <- System.getProperties.asScala) {
      println(k);
    }
    println("------------------------")
    for((k, v) <- System.getProperties.asScala if v == "") {
     println(k);
    }

    // 样咧类
    abstract class Amount;
    case class Dollar(value : Double) extends Amount;
    case class Currency(value : Double, unit : String) extends Amount;
    case object Nothing extends Amount;
    /*  当你声明样例类时，如下几件事情会自动发生：
    ——构造其中的每一个参数都成为val
    ——除非它被显式地声明为var（不建议这样做）
    ——在半生对象中提供apply方法让你不用new关键字就能构造出相应的对象，比如Dollar(29.95) 或Currency(29.95,“EUR”)
    ——提供unapply方法让模式匹配可以工作
    ——将生成toString、equals、hashCode和copy方法
    ——除非显式地给出这些方法的定义。
    除上述外，样例类和其他类型完全一样。你可以添加方法和字段，扩展它们。*/
    for(amt <- Array(Dollar(1000.0), Currency(1000.0, "EUR"), Nothing)) {
      val res = amt match {
        case Dollar(v) => "$" + v;
        case Currency(_, u) => u;
        case Nothing =>
      }
      println(amt + ": " + res);
    }
    // Copy方法和带名参数，copy创建一个与现有对象值相同的新对象，并可以通过带名参数来修改某些属性。
    val amt = Currency(29.95, "EUR");
    val price = amt.copy(value = 18.95);
    println(amt);
    println(price);
    println(amt.copy(unit = "CHF"));
    // Case语句的中置(缀)表达式
    List(1, 7, 2, 9) match {
      case f :: s :: t :: rest => println(f + "  " + s + " " + t + " " + rest.length);
      case _ => 0
    }
    // 匹配嵌套结构
    abstract class Item;
    case class Article(description : String, price : Double) extends Item;
    case class Bundle(description : String, discount : Double, item : Item*) extends Item;
    val sale = Bundle("sss", 10, Article("aa", 40), Bundle("aaa", 20, Article("ss", 80), Article("s", 160)));
    // 将desc绑定到第一个Article的描述
    val res1 = sale match {
      case Bundle(_, _, Article(desc, _), _*) => desc;
    }
    println(res1);
    // 通过 @表示法将嵌套的值绑定到变量 。_ * 绑定剩余Item到rest
    val res2 = sale match {
      case Bundle(_, _, art @ Article(_ , _), rest @ _*) => (art, rest);
    }
    println(res2);
    // 不使用_*绑定剩余Item到rest
    val res3 = sale match {
      case Bundle(_, _, art @ Article(_ , _), rest) => (art, rest);
    }
    println(res3);
    // 计算某个Item价格的函数，并调用
    def price_(it : Item) : Double = {
      it match {
        case Article(_, p) => p;
        case Bundle(_, disc, its @ _*) => its.map(x => price_(x)).sum - disc;
      }
    }
    println(price_(sale));
    // 模拟枚举
    sealed abstract class TrafficLightColor;
    case object Red extends TrafficLightColor;
    case object Yellow extends TrafficLightColor;
    case object Green extends TrafficLightColor;
    for(color <- Array(Red, Yellow, Green)) {
      println(
        color match {
          case Red => "stop";
          case Yellow => "slowly";
          case Green => "pass"
        }
      )
    }
    // 偏函数 ： 偏函数，它只对会作用于指定类型的参数或指定范围值的参数实施计算
    val f1 :  PartialFunction[Char, Int] = {
      case '+' => 1;
      case '-' => -1;
      case _ => 0;
    }
    println(f1('_'));
    println(f1.isDefinedAt('0'));
    println(f1('+'));

    val f2 = new PartialFunction[Any, Int] {
      def apply(any : Any) = any.asInstanceOf[Int] + 1;
      def isDefinedAt(any: Any) = if(any.isInstanceOf[Int]) true else false;
    }
    val rf1 = List(1, 2, 3, 5, "six") collect f2;
    println(rf1);

    def f3 : PartialFunction[Any, Int] = {
      case i : Int => i + 1;
    }
    val rf2 = List(1, 2, 3, 5, "six") collect f3;
    println(rf2);
  }
  // 高阶函数
  def high_func(): Unit = {
    // 作为参数的高阶函数
    def plus(x : Int) = 3 + x;
    val res = Array(1, 2, 3, 4).map(plus(_));
    println(res.mkString(","));

    // 匿名函数
    val triple = (x : Double) => 3 * x;
    println(triple(3));

    // 能够接受函数作为参数的函数，叫做高阶函数。
    def fun1(f : Double => Double) = f(10);
    def fun2(x : Double) = x - 7;
    val res2 = fun1(fun2);
    println(res2);
    // 高阶函数同样可以返回函数类型
    def fun3(x : Int) = (y : Int) => x - y;
    val res3 = fun3(3)(5);
    println(res3);

    // 参数(类型)推断
    // 传入函数表达式
    println(fun1((x : Double) => 3 * x));
    // 参数推断省区类型信息
    println(fun1((x) => 3 + x));
    // 单个参数可以省去括号
    println(fun1(x => 5 + x));
    // 如果变量值在=>右边只出现一次，可以使用_代替
    println(fun1(3 * _));

    // 闭包, 闭包就是一个函数把外部的那些不属于自己的对象也包含(闭合)进来。
    def fun4(x : Int) = (y : Int) => x -y;
    val f1 = fun4(10);
    val f2 = fun4(10);
/*    1) 匿名函数(y: Int)=> x - y嵌套在minusxy函数中。
    2) 匿名函数(y: Int)=> x - y使用了该匿名函数之外的变量x
    3) 函数fun4返回了引用了局部变量的匿名函数
    f1, f2这两个函数就叫闭包。*/
    println(f1(3) + f2(2));

    // 柯里化， 函数编程中，接受多个参数的函数都可以转化为接受单个参数的函数，这个转化过程就叫柯里化，柯里化就是证明了函数只需要一个参数而已
    def fun5(x : Int, y : Int) = x * y;
    println(fun5(3, 3));
    def fun6(x : Int) = (y : Int) => x * y;
    println(fun6(3)(5));
    def fun7(x : Int)(y : Int) = x * y;
    println(fun7(3)(3));

    /* 控制抽象
    1、参数是函数。
    2、函数参数没有输入值也没有返回值。*/
    def fun_thread1(f1 :  => Unit) : Unit = {
      new Thread{
        override def run(): Unit = {
          f1
        }
      }.start();
    }

    fun_thread1{
      println("run 1");
      Thread.sleep(5000);
      println("run 2")
    }

    def fun_thread2(f2 : => Boolean)(block : => Unit): Unit = {
      if(!f2) {
        block
        fun_thread2(f2)(block);
      }
    }
    var x = 10;
    fun_thread2(x == 0) {
      x -= 1;
      println(x);
    }
  }
}

object Demo{
  def main(args : Array[String]) : Unit = {
    // 调用java函数
    val test = new Test();
    println(test.ss());
    var d = new Demo();
    d.high_func();
  }
}
