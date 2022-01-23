package com.hl.bigdata.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * hbase客户端接口操作hbase基本的增删改查
 * @author huanglin
 * @date 2022年1月12日 下午11:02:21
 *
 */
public class ClientCRUD {
	
	private Connection conn;
	private Configuration conf;
	private Admin      admin;
	
	/**
	 * 初始连接
	 * @throws IOException
	 */
	@Before
	public void initConnection() throws IOException {
		conf  = HBaseConfiguration.create();
		conn  = ConnectionFactory.createConnection(conf);
		admin = conn.getAdmin();
	}
	
	/**
	 * 创建命名空间
	 * @throws IOException
	 */
	@Test
	public void createNamespace() throws IOException {
		NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("test2");
		NamespaceDescriptor build = builder.build();
		admin.createNamespace(build);
		admin.close();
	}
	
	/**
	 * 删除命名空间
	 * @throws IOException
	 */
	@Test
	public void dropNamespace() throws IOException {
		admin.deleteNamespace("test2");
		admin.close();
	}
	
	/**
	 * 创建表
	 * @throws IOException
	 */
	@Test
	public void createTable() throws IOException {
		// 创建表描述符
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("test:t2"));
		// 创建列族描述符
		HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes("cf1"));
		// 添加列族描述符
		desc.addFamily(colDesc);
		// 创建表
		admin.createTable(desc);
	}
	
	/**
	 * 删除表
	 * @throws IOException
	 */
	@Test
	public void dropTable() throws IOException {
		// 先禁用表
		admin.disableTable(TableName.valueOf("test:t2"));
		// 删除表
		admin.deleteTable(TableName.valueOf("test:t2"));
		admin.close();
	}
	
	/**
	 * 插入数据单行数据
	 */
	@Test
	public void put() {
		try {
			Table t   = conn.getTable(TableName.valueOf("test:t2"));
			Put   put = new Put(Bytes.toBytes("row1"));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes("0001"));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("jack"));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(17));
			t.put(put);
			t.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 插入集合数据，多行数据 
	 */
	@Test
	public void puts() {
		try {
			Table     t    = conn.getTable(TableName.valueOf("test:t2"));
			List<Put> puts = new ArrayList<Put>();
			for(int i = 2; i < 5; i++ ) {
				Put put = new Put(Bytes.toBytes("row" + i));
				put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes("000" + i));
				put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("jack" + i));
				put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(17 + i));
				puts.add(put);
			}
			t.put(puts);
			t.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 更新数据
	 */
	@Test
	public void update() {
		try {
			Table t   = conn.getTable(TableName.valueOf("test:t2"));
			Put   put = new Put(Bytes.toBytes("row1"));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
			t.put(put);
			t.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除数据
	 */
	@Test
	public void delete() {
		try {
			Table  t   = conn.getTable(TableName.valueOf("test:t2"));
			Delete del = new Delete(Bytes.toBytes("row1"));
			del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
			t.delete(del);
			t.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取数据
	 */
	@Test
	public void get() {
		try {
			Table t   = conn.getTable(TableName.valueOf("test:t2"));
			Get   get = new Get(Bytes.toBytes("row1"));
			get.addFamily(Bytes.toBytes("cf1"));
			Result result = t.get(get);
			String no     = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no")));
			System.out.println(no);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取批量数据
	 */
	@Test
	public void getAll() {
		try {
			Table            t        = conn.getTable(TableName.valueOf("test:t2"));
			Scan             scan     = new Scan(Bytes.toBytes("row1"), Bytes.toBytes("row4")); // 查询row1到row3的所有数据
			ResultScanner    scanner  = t.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			while(iterator.hasNext()) {
				Result next = iterator.next();
				String no   = Bytes.toString(next.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no")));
				String name = Bytes.toString(next.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
				int    age  = Bytes.toInt(next.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")));
				
				System.out.println("no : " + no + ", name : " + name + ", age: " + age);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 测试客户端缓冲区间 2M
	 * 十万条记录：  170312
	 * @throws Exception
	 */
	@Test
	public void clientBuffer() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Put    put   = null;
		long   start = System.currentTimeMillis();
		for(int i = 1; i < 100000; i++) {
			put = new Put(Bytes.toBytes("row" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes("000" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("jack" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(i));
			table.put(put);
		}
		long end = System.currentTimeMillis();
		System.out.println(end - start);
		table.close();
	}

	/**
	 * 测试客户端缓冲区间 2M，手动清理缓存
	 * 十万条数据 9708
	 * @throws Exception
	 */
	@Test
	public void clinetBuffer2() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Put    put   = null;
		long   start = System.currentTimeMillis();
		table.setAutoFlush(false); // 关闭自动清理
		for(int i = 100001; i < 200000; i++) {
			put = new Put(Bytes.toBytes("row" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes("000" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("jack" + i));
			put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(i));
			table.put(put);
		}
		table.flushCommits();
		long end = System.currentTimeMillis();
		System.out.println(end - start);
		table.close();
	}

	/**
	 * checkPut检查并执行.节省rpc时间
	 * @throws Exception
	 */
	@Test
	public void checkPut() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Put    put   = new Put(Bytes.toBytes("row10000"));
		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("jack10001"));
		boolean checkAndPut = table.checkAndPut(Bytes.toBytes("row10000"), Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("jack10001"), put);
		System.out.println(checkAndPut);
	}


	/**
	 * 判断数据是否存在
	 * @throws Exception
	 */
	@Test
	public void exists() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Get    get   = new Get(Bytes.toBytes("row10000"));
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		get.setTimeRange(1642920979026L, 1642920979027L);
		boolean exists = table.exists(get);
		System.out.println(exists);
	}

	/**
	 * 扫描器缓存
	 * setCaching 1    -> 136409
	 * setCaching 100  -> 3956
	 * setCaching 1000 -> 2402
	 * @throws Exception
	 */
	@Test
	public void scannerCaching() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan   scan  = new Scan();
		scan.setCaching(1000);
		scan.setStartRow(Bytes.toBytes("row300000"));
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		long start = System.currentTimeMillis();
		scannerOutput(table.getScanner(scan));
		long end = System.currentTimeMillis();
		System.out.println(end - start);
	}

	/**
	 * bacth操作
	 * @throws Exception
	 */
	@Test
	public void batch() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan   scan  = new Scan();
		System.out.println(scan.getBatch());
		scan.setBatch(1000);
		scan.setCaching(1000);
		scan.setStartRow(Bytes.toBytes("row300000"));
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		long start = System.currentTimeMillis();
		scannerOutput(table.getScanner(scan));
		long end = System.currentTimeMillis();
		System.out.println(end - start);
	}

	/**
	 * 基于限定符Qualifier（列）过滤数据(过滤返回列)
	 * @throws Exception
	 */
	@Test
	public void filterByQualifier() throws Exception {
		HTable          table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Get             get    = new Get(Bytes.toBytes("row1"));
		QualifierFilter filter = new QualifierFilter(CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("name")));
		get.setFilter(filter);
		Result                       result    = table.get(get);
		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("cf1"));
		for(Entry<byte[], byte[]> entry : familyMap.entrySet()) {
			System.out.println(Bytes.toString(entry.getKey()) + " : " + Bytes.toString(entry.getValue()));
		}
	}

	/**
	 * 基于限定符Qualifier（列）过滤数据(过滤返回列)
	 * @throws Exception
	 */
	@Test
	public void filterByQualifier2() throws Exception {
		HTable          table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan            scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		scan.addFamily(Bytes.toBytes("cf1"));
		QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name")));
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 值进行过滤，用法和RowFilter类似，只不过侧重点不同而已，针对的是单元值，使用这个过滤器可以过滤掉不符合设定标准的所有单元
	 * @throws Exception
	 */
	@Test
	public void filterByValue() throws Exception{
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan   scan  = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		scan.addFamily(Bytes.toBytes("cf1"));
		ValueFilter filter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("000100041")));
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 列值过滤器,会返回改行所有列数据
	 * @throws Exception
	 */
	@Test
	public void filterBySingleColumnValue() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan   scan  = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		scan.addFamily(Bytes.toBytes("cf1"));
		byte[] f = Bytes.toBytes("cf1");
		byte[] c = Bytes.toBytes("age");

		SingleColumnValueFilter filter = new SingleColumnValueFilter(f, c, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(10010)));
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 列值过滤器,会返回除指定的列的其他列数据
	 * @throws Exception
	 */
	@Test
	public void filterBySingleColumnValueExclude() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan   scan  = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		scan.addFamily(Bytes.toBytes("cf1"));
		byte[] f = Bytes.toBytes("cf1");
		byte[] c = Bytes.toBytes("age");

		SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(f, c, CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(10010)));
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 过滤器链 ，它可以包含一组即将应用于目标数据集的过滤器 ，过滤器间具有“与” 关系，只要满足其中一个即可
	 * @throws Exception
	 */
	@Test
	public void filterByList() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan   scan  = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		scan.addFamily(Bytes.toBytes("cf1"));
		ValueFilter filter1 = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("000100041")));
		ValueFilter filter2 = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("000100031")));
		FilterList  list    = new FilterList(Operator.MUST_PASS_ONE);
		list.addFilter(filter1);
		list.addFilter(filter2);
		scan.setFilter(list);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 复杂过滤器组合查询
	 * @throws Exception
	 */
	@Test
	public void complexFilter() throws Exception {
		HTable table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan   scan  = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		byte[] f     = Bytes.toBytes("cf1");
		byte[] c1    = Bytes.toBytes("name");
		byte[] c2    = Bytes.toBytes("age");

		// name like 'jack%'
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(f, c1, CompareOp.EQUAL, new RegexStringComparator("^jack"));
		// age > 10500
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(f, c2, CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(10500)));
		FilterList              fl1     = new FilterList(Operator.MUST_PASS_ALL, filter1, filter2);

		// name like '%t'
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(f, c1, CompareOp.EQUAL, new RegexStringComparator("t$"));
		// age < 10200
		SingleColumnValueFilter filter4 = new SingleColumnValueFilter(f, c2, CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(10200)));
		FilterList              fl2     = new FilterList(Operator.MUST_PASS_ALL, filter3, filter4);

		FilterList fl3 = new FilterList(Operator.MUST_PASS_ONE, fl1, fl2);
		scan.setFilter(fl3);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * prefixFilter:判断rowkey
	 * @throws Exception
	 */
	@Test
	public void prefixFilter() throws Exception {
		HTable       table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan         scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes("row1050"));
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 指定页面行数，返回对应行数的结果集需要注意的是，该过滤器并不能保证返回的结果行数小于等于指定的页面行数，因为过滤器是分别作用到各个region server的，它只能保证当前region返回的结果行数不超过指定页面行数
	 * @throws Exception
	 */
	@Test
	public void filterByPage() throws Exception {
		HTable     table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan       scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		PageFilter filter = new PageFilter(10);
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * KeyOnlyFilter,只返回坐标值(rowkey family column ts),没有value，value长度0
	 * @throws Exception
	 */
	@Test
	public void filterByKeyOnly() throws Exception {
		HTable        table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan          scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		KeyOnlyFilter filter = new KeyOnlyFilter();
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * FirstKeyOnlyFilter,类似于keyOnlyFilter,但是只返回第一列类似于keyOnlyFilter,但是只返回第一列
	 * @throws Exception
	 */
	@Test
	public void filterByFirstKeyOnly() throws Exception {
		HTable             table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan               scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * InclusiveStopFilter，包含结束行。构造是rowkey
	 * @throws Exception
	 */
	@Test
	public void filterByInclusiveStop() throws Exception {
		HTable              table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan                scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		InclusiveStopFilter filter = new InclusiveStopFilter(Bytes.toBytes("row10010"));
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 时间戳过滤器,检索cell的时间戳符合list表中定义的时间戳
	 * @throws Exception
	 */
	@Test
	public void filterByTimestamp() throws Exception {
		HTable     table = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan       scan  = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		List<Long> list  = new ArrayList<Long>();
		list.add(1642920077547L);
		list.add(1642920077579L);
		TimestampsFilter filter = new TimestampsFilter(list);
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * columnCountGetFilter,检索开头的n列数据，而且只返回一行
	 * @throws Exception
	 */
	@Test
	public void filterByColumnGet() throws Exception {
		HTable               table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan                 scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		ColumnCountGetFilter filter = new ColumnCountGetFilter(2);
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * columnCountGetFilter,类似于columnCountGetFilter
	 * (1, 2) == (limit , offset)
	 * offset : 跳过的列数
	 * limit  : 取的列数
	 * 返回多行
	 * @throws Exception
	 */
	@Test
	public void filterByPagination() throws Exception {
		HTable                 table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan                   scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		ColumnPaginationFilter filter = new ColumnPaginationFilter(1, 2);
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * ColumnPrefixFilter,对列名进行过滤,过滤列名以指定参数为前缀的column
	 * @throws Exception
	 */
	@Test
	public void filterByColumnPrefix() throws Exception {
		HTable             table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan               scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));	
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * randomRowFilter，行随机过滤器，随选中行，几率是参数.0:永远选不中，1:一定选中
	 * @throws Exception
	 */
	@Test
	public void filterByRandom() throws Exception {
		HTable          table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan            scan   = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		RandomRowFilter filter = new RandomRowFilter(0.1f);
		scan.setFilter(filter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * skipFilter:装饰模式,需要对指定的过滤器进行包装
	 * @throws Exception
	 */
	@Test
	public void filterBySkip() throws Exception {
		HTable                  table      = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan                    scan       = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		SingleColumnValueFilter filter     = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("age"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(10990));
		SkipFilter              skipFilter = new SkipFilter(filter);
		scan.setFilter(skipFilter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 全匹配过滤器, 检索到第一个不满足filter1的cell就终止，返回检索的结果
	 * @throws Exception
	 */
	@Test
	public void filterByWhileMatch() throws Exception {
		HTable           table       = (HTable) conn.getTable(TableName.valueOf("test:t2"));
		Scan             scan        = new Scan(Bytes.toBytes("row10000"), Bytes.toBytes("row11000"));
		RowFilter        filter      = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row10055")));
		WhileMatchFilter whileFilter = new WhileMatchFilter(filter);
		scan.setFilter(whileFilter);
		scannerOutput(table.getScanner(scan));
	}

	/**
	 * 自定义顾虑器
	 * @throws Exception
	 */
	@Test
	public void filterBySelfFilter() {
		try {
			HTable         table  = (HTable) conn.getTable(TableName.valueOf("test:t2"));
			Scan           scan   = new Scan();
			SelfLessFilter filter = new SelfLessFilter(Bytes.toBytes(11000));
			scan.setFilter(filter);
			scannerOutput(table.getScanner(scan));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void scannerOutput(ResultScanner result) {
		Iterator<Result> iterator = result.iterator();
		while(iterator.hasNext()) {
			Result next = iterator.next();
			NavigableMap<byte[], byte[]> familyMap = next.getFamilyMap(Bytes.toBytes("cf1"));
			for(Entry<byte[], byte[]> map : familyMap.entrySet()) {
				System.out.println(Bytes.toString(map.getKey()) + " : " + Bytes.toString(map.getValue()) + "\n");
			}
		}
	}
}
