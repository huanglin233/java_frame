package com.hl.bigdata.hive.userFunctions;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive中讲字符串转化为time
 * @author huanglin
 * @date 2022年1月8日 下午3:05:13
 *
 */
@Description(name ="to_date", value = "udf date functions", extended = "For Example : select to_date(2022-1-8 15:27:33)")
public class ToDate extends UDF {

	/**
	 * 计算
	 */
	public Date evaluate(String date) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat();
			sdf.applyPattern("yyyy-MM-dd HH:mm:ss");
			
			return sdf.parse(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new Date();
	}
}
