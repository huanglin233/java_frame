package com.hl.bigdata.hadoop.mr;

import java.net.InetAddress;

/**
 * util tools
 * 
 * @author huanglin
 * @date 2021/08/05 21/50/10
 */
public class Util {

    /**
     * 获得组名
     */
    public static String getGrp(String grp, int hash) {
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            long   time     = System.nanoTime();

            return "[" + hostName + "]." + hash + "." + time + " : " + grp;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获得组名
     */
    public static String getGrp2(String grp, int hash) {
        try {
            String hostName = InetAddress.getLocalHost().getHostName();

            return "[" + hostName + "]." + hash +  " : " + grp;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}