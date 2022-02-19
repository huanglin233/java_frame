package com.hl.bigdata.serializeLib;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import com.hl.bigdata.serializeLib.vo.SelfUser;
import com.hl.bigdata.serializeLib.vo.User;
import com.hl.bigdata.serializeLib.vo.UserProtos;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/* * 
 * 串行化测试
 * @Author: huanglin 
 * @Date: 2022-02-19 18:50:47 
 */ 
public class SerializePerformance {

    private static int MAX = 100000;
    
    public static void main(String[] args) throws Exception {
        javaSerialize();
        // writableSerialize();
        // avroSerialize();
        // protobuf();
        /**
         *                     time  size
         * javaSerialize:     20588  4177989
         * writableSerialize: 30001  2777780
         * avroSerialize:     2147   2670661
         * protobufSerialize: 14975  2461266
         */
    }

    /**
     * java的串行化
     * @throws Exception
     */
    private static void javaSerialize() throws Exception {
        long               start = System.currentTimeMillis();
        FileOutputStream   fos   = new FileOutputStream("./src/main/java/com/hl/bigdata/serializeLib/data/users.java_data");
        ObjectOutputStream oos   = new ObjectOutputStream(fos);
        SelfUser           user  = null;
        for(int i = 0; i < MAX; i++) {
            user = new SelfUser();
            user.setName("jack" + i);
            user.setFavoriteNumber(i);
            user.setFavoriteColor("yellow" + i);
            oos.writeObject(user);
        }
        oos.close();
        System.out.println("javaSerialize: " + (System.currentTimeMillis() - start));
    }

    /**
     * writable的串行化
     */
    private static void writableSerialize() throws Exception {
        long             start = System.currentTimeMillis();
        FileOutputStream fos   = new FileOutputStream("./src/main/java/com/hl/bigdata/serializeLib/data/users.writable");
        DataOutputStream dos   = new DataOutputStream(fos);
        SelfUser         user  = null;
        for(int i = 0; i < MAX; i++) {
            user = new SelfUser();
            user.setName("jack" + i);
            user.setFavoriteNumber(i);
            user.setFavoriteColor("yellow" + i);
            user.write(dos);
        }
        dos.close();
        System.out.println("writableSerialize: " + (System.currentTimeMillis() - start));
    }

    /**
     * avro的串行化
     * @throws Exception
     */
    private static void avroSerialize() throws Exception {
        long                 start      = System.currentTimeMillis();
        FileOutputStream     fos        = new FileOutputStream("./src/main/java/com/hl/bigdata/serializeLib/data/users.avro");
        DatumWriter<User>    writer     = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> fileWriter = new DataFileWriter<User>(writer);
        User                 user       = new User();
        fileWriter.create(user.getSchema(), fos);
        for(int i = 0; i < MAX; i++) {
            User user01 = new User();
            user01.setName("jack" + i);
            user01.setFavoriteNumber(i);
            user01.setFavoriteColor("yellow" + i);
            fileWriter.append(user01);
        }
        fileWriter.close();
        System.out.println("avroSerialize: " + (System.currentTimeMillis() - start));
    }

    /**
     * protobuf的串行化
     * @throws Exception
     */
    private static void protobuf() throws Exception {
        long                 start      = System.currentTimeMillis();
        FileOutputStream     fos        = new FileOutputStream("./src/main/java/com/hl/bigdata/serializeLib/data/users.protobuf");
        UserProtos.User      user       = null;
        for(int i = 0; i < MAX; i++) {
            user = UserProtos.User.newBuilder().setFavoriteNumber(i).setName("jack" + i).setFavoriteColor("red" + i).build();
            user.writeTo(fos);
        }
        fos.close();
        System.out.println("protobufSerialize: " + (System.currentTimeMillis() - start));
    }
}
