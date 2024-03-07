package com.hl.bigdata.hive.userFunctions;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * 自定义for循环函数-hvie的udtf
 * @author huanglin
 * @date 2022年1月8日 下午3:47:10
 *
 */
@Description(name ="forx", value = "udtf forx functions", extended = "For Example : forx(1, 3, 1)")
public class ForEachUDTF extends GenericUDTF {
	
	IntWritable start; // 开始
	IntWritable end; // 结束
	IntWritable inc; // 增量
	
	private Object[] forwardObj;
	
	/**
	 * 初始化函数
	 */
	public StructObjectInspector initialize(ObjectInspector[] args) {
		start = ((WritableConstantIntObjectInspector) args[0]).getWritableConstantValue();
		end   = ((WritableConstantIntObjectInspector) args[1]).getWritableConstantValue();
		if(args.length == 3) {
			inc = ((WritableConstantIntObjectInspector) args[2]).getWritableConstantValue();
		} else {
			inc = new IntWritable(1);
		}
		this.forwardObj = new Object[1];
		
		ArrayList<String>          fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIS   = new ArrayList<ObjectInspector>();
		fieldNames.add("col0");
		
		fieldOIS.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.INT));
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIS);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		for(int i = start.get(); i < end.get(); i = i + inc.get()) {
			this.forwardObj[0] = new Integer(i);
			forward(forwardObj);
		}
	
	}

	@Override
	public void close() throws HiveException {
	}
}
