package com.hl.bigdata.serializeLib;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

import com.hl.bigdata.serializeLib.vo.SelfUser;
import com.hl.bigdata.serializeLib.vo.User;
import com.hl.bigdata.serializeLib.vo.UserProtos;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

/* * 
 * 反序列化测试
 * @Author: huanglin 
 * @Date: 2022-02-19 20:20:58 
 */ 
public class DesceriablizePerformance {
    
    private static int MAX = 100000;
    
    public static void main(String[] args) throws Exception {
        javaDeseriablize();
        writableDeserablize();
        avroDeserablize();
        protobufDeseriablize();
        /**
         *                     time  size      describeTime
         * javaSerialize:     20588  4177989   2106
         * writableSerialize: 30001  2777780   1259
         * avroSerialize:     2147   2670661   1544
         * protobufSerialize: 14975  2461266   1934
         */
    }

    /**
     * java的反序列化
     * @throws Exception
     */
    private static void javaDeseriablize() throws Exception {
        long              start = System.currentTimeMillis();
        FileInputStream   in    = new FileInputStream("./src/main/java/com/hl/bigdata/serializeLib/data/users.java_data");
        ObjectInputStream ois   = new ObjectInputStream(in);
        SelfUser          user  = new SelfUser();
        for(int i = 0; i < MAX; i++) {
            user = (SelfUser) ois.readObject();
        }
        ois.close();
        System.out.println("javaDeseriablize：" + (System.currentTimeMillis() - start));
    }

    /**
     * writable反序列化
     * @throws Exception
     */
    private static void writableDeserablize() throws Exception {
        long              start = System.currentTimeMillis();
        FileInputStream   in    = new FileInputStream("./src/main/java/com/hl/bigdata/serializeLib/data/users.writable");
        DataInputStream   dis   = new DataInputStream(in);
        SelfUser          user  = new SelfUser();
        for(int i = 0; i < MAX; i++) {
            user.readFields(dis);
        }
        dis.close();
        System.out.println("writableDeseriablize：" + (System.currentTimeMillis() - start));
    }

    /**
     * avro反序列化
     * @throws Exception
     */
    private static void avroDeserablize() throws Exception {
        long                start       = System.currentTimeMillis();
        File                 file       = new File("./src/main/java/com/hl/bigdata/serializeLib/data/users.avro");       
        DatumReader<User>    reader     = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> fileReader = new DataFileReader<User>(file, reader);
        User                 user       = null;
        while(fileReader.hasNext()) {
            user = fileReader.next();
        }
        fileReader.close();
        System.out.println("avroDeseriablize：" + (System.currentTimeMillis() - start));
    }

    /**
     * protobuf反序列化
     * @throws Exception
     */
    private static void protobufDeseriablize() throws Exception {
        long              start = System.currentTimeMillis();
        FileInputStream   in    = new FileInputStream("./src/main/java/com/hl/bigdata/serializeLib/data/users.protobuf");
        for(int i = 0; i < MAX; i++) {
            com.hl.bigdata.serializeLib.vo.UserProtos.User parseFrom = UserProtos.User.parseFrom(in);
        }
        System.out.println("protobufDeseriablize：" + (System.currentTimeMillis() - start));
    }
}
