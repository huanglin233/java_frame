package com.hl.bigdata.serializeLib.avro;

import java.io.File;

import com.hl.bigdata.serializeLib.vo.User;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

/* * 
 * avro 反序列化
 * @Author: huanglin 
 * @Date: 2022-02-16 21:58:03 
 */ 
public class Deserialize {
    
    public static void main(String[] args) throws Exception {
        File                 file            = new File("./src/main/java/com/hl/bigdata/serializeLib/avro/users.avro");
        DatumReader<User>    datumReader     = new SpecificDatumReader<User>();
        DataFileReader<User> datumFileReader = new DataFileReader<User>(file, datumReader);
        User                 user            = null;

        while(datumFileReader.hasNext()) {
            user = datumFileReader.next(user);
            System.out.println(user.getName() + ", " + user.getFavoriteNumber() + ", " + user.getFavoriteColor());
        }
    }

}
