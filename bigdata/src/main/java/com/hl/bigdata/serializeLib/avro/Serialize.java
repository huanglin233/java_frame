package com.hl.bigdata.serializeLib.avro;

import java.io.File;

import com.hl.bigdata.serializeLib.vo.User;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/* * 
 * avro序列化
 * @Author: huanglin 
 * @Date: 2022-02-15 22:44:24 
 */ 
public class Serialize {

    public static void main(String[] args) throws  Exception{
        User user1 = new User();
        user1.setName("jack");
        user1.setFavoriteNumber(888);
        user1.setFavoriteColor("blue");
        User user2 = User.newBuilder().setName("jack2").setFavoriteNumber(999).setFavoriteColor("blue").build();

        // 创建写入器
        DatumWriter<User>    writer     = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> fileWriter = new DataFileWriter<User>(writer);
        fileWriter.create(user1.getSchema(), new File("./src/main/java/com/hl/bigdata/serializeLib/avro/users.avro"));
        // 开始写入数据
        fileWriter.append(user1);
        fileWriter.append(user2);
        // 关闭写入
        fileWriter.close();

        System.out.println("写入完成");
    }
}
