package com.hl.bigdata.scala.generics

/**
 * @author huanglin
 * @date 2022/03/10 23:56:09
 */
// 视图界定 T <% V要求必须存在一个从T到V的隐式转换。上下文界定的形式为T:M，其中M是另一个泛型类，它要求必须存在一个类型为M[T]的隐式值。
// 下面类定义要求必须存在一个类型为Ordering[T]的隐式值，当你使用了一个使用了隐式值得方法时，传入该隐式参数。
class Pair4[T : Ordering](val first : T, val second : T) {

  def smaller(implicit ord : Ordering[T]): Unit = {
   println(ord)
   if(ord.compare(first, second) < 0) first else second;
  }

  override def toString: String =  {
    "(" + first + "," + second + ")"
  }

  def foo[T](x : List[T])(implicit m : Manifest[T]): Unit = {
    println(m);
//    =:=，意思为：type equality
//    <:< ,意思为：subtype relation
//    类型判断不要用 == 或 !=
    if(m.equals(manifest[String])) {
      println("this list is full of strings");
    } else {
      println("Non-stringy list");
    }
  }

//  不能同时有多个上界或下界，变通的方式是使用复合类型
//  T <: A with B
//  T >: A with B
//  可以同时有上界和下界，如
//  T >: A <: B
//  这种情况下界必须写在前边，上界写在后边，位置不能反。同时A要符合B的子类型，A与B不能是两个无关的类型。
//  可以同时有多个view bounds
//    T <% A <% B
//  这种情况要求必须同时存在 T=>A的隐式转换，和T=>B的隐式转换。
  class A{}
  class B{}
  implicit def string2A(s:String) = new A
  implicit def string2B(s:String) = new B
  def foo2[ T <% A <% B](x:T)  = println( x + " OK")
  def foo3(): Unit = {
    foo2("ss");
  }

//  可以同时有多个上下文界定
//  T : A : B
//  这种情况要求必须同时存在C[T]类型的隐式值，和D[T]类型的隐式值。

  class C[T];
  class D[T];
  implicit val c = new C[Int]
  implicit val d = new D[Int]
  def foo4[ T : C : D ](i:T) = println(i + " OK")
  def foo5(): Unit = {
    foo4(1);
  }
}

object Pair4 extends App {
  val p = new Pair4(1, 2);
  println(p.smaller);
  p.foo(List("one", "tow"));
  p.foo(List(1, 2));
  p.foo(List("one", 2));
  p.foo3();
  p.foo5();

  implicit def a(d: Double) = d.toInt
  val i1: Int = 3.5
  println(i1)

  def foo(msg : String) = println(msg);
  implicit def intToString(x : Int) = x.toString;
  foo(10);
  def foo2(s : Double): Unit = {
    println(s);
  }
  implicit def intToString2(x : String) = x.toDouble;
  foo2("1");

  import com.hl.bigdata.scala.generics.StringUtils._
  println("sas".icrement);
}

//在scala2.10后提供了隐式类，可以使用implicit声明类，但是需要注意以下几点：
//—– 其所带的构造参数有且只能有一个
//—– 隐式类必须被定义在“类”或“伴生对象”或“包对象”里
//—– 隐式类不能是case class（case class在定义会自动生成伴生对象与2矛盾）
//—– 作用域内不能有与之相同名称的标示符

object StringUtils {
  implicit class StringImprovement(val s : String) { // 隐式类
    def icrement = s.map(x => (x + 1).toChar)
  }
}