package com.hl.bigdata.hadoop.mr.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 比较器
 * @author huanglin
 * @date 2021/08/08 15/39/05
 */
public class CIDGroupComparator extends WritableComparator{

    public CIDGroupComparator() {
        super(ComboKey.class, true);
    }

    public int compare(WritableComparable wca, WritableComparable wcb) {
        ComboKey ck1 = (ComboKey) wca;
        ComboKey ck2 = (ComboKey) wcb;

        return ck1.getCustomerId() - ck2.getCustomerId();
    }
}
