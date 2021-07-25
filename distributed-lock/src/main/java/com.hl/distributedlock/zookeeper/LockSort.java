package com.hl.distributedlock.zookeeper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * zk中顺序临时节点排序
 *
 * @author huanglin
 * @date 2021/5/29 下午6:26
 */
public class LockSort implements Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
        return Integer.compare(getInt(o1), getInt(o2));
    }

    public int getInt(String str) {
        String[] locks  = str.split("lock");
        int      i      = locks[1].lastIndexOf("0");
        String   numStr = locks[1].substring(i - 1);

        return Integer.parseInt(numStr);
    }

    public static void main(String[] args) {
        LockSort lockSort = new LockSort();
        System.out.println(lockSort.getInt("lock0000000044"));

        List<String> list = new ArrayList<String>();
        list.add("lock0000000044");
        list.add("lock0000000045");
        list.add("lock0000000043");
        list.add("lock0000000042");
        list.add("lock0000000048");
        System.out.println(list.toString());
        list.sort(new LockSort());
        System.out.println(list.toString());
    }
}
