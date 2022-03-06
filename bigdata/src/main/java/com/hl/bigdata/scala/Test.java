package com.hl.bigdata.scala;

/**
 * @author huanglin
 * @date 2022/03/02 22:23:8
 */
public class Test {
    public String ss() {
        return "aa";
    }

    public static void main(String[] args) {
        // 调用scala函数
        Demo demo = new Demo();
        System.out.println(demo.add(1 , 1));
    }
}
