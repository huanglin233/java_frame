package com.hl.bigdata.serializeLib.protobuf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.hl.bigdata.serializeLib.vo.BookProtos.Person;

/* * 
 * protobuf反序列
 * @Author: huanglin 
 * @Date: 2022-02-19 18:13:16 
 */ 
public class Desceriablize {
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        Person parseFrom = Person.parseFrom(new FileInputStream("./src/main/java/com/hl/bigdata/serializeLib/protobuf/person_protobuf.data"));
        System.out.println(parseFrom.getEmial());
    }
}
