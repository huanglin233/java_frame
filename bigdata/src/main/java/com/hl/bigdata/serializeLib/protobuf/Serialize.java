package com.hl.bigdata.serializeLib.protobuf;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.hl.bigdata.serializeLib.vo.BookProtos;
import com.hl.bigdata.serializeLib.vo.BookProtos.Person;

/* * 
 * protobuf序列化
 * @Author: huanglin 
 * @Date: 2022-02-18 00:12:44 
 */ 
public class Serialize {
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        Person bookProtos = BookProtos.Person.newBuilder().setId(12345).setName("jack").setEmial("jack@em.com")
                                                .addPhone(BookProtos.Person.PhoneNumber.newBuilder().setNumber("+135 999 999 999").setRtpe(BookProtos.Person.PhoneType.HOME).build()).build();
        bookProtos.writeTo(new FileOutputStream("./src/main/java/com/hl/bigdata/serializeLib/protobuf/person_protobuf.data"));
    }
}
