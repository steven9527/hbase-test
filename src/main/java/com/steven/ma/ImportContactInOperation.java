package com.steven.ma;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.CharSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.http.impl.io.SocketOutputBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.steven.db.mysql.MySqlHelper;
import com.sun.xml.internal.ws.util.ReadAllStream;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.PreparedStatement;

public class ImportContactInOperation {

	private final static String TABLE_NAME = "contact_in_operation_all";

	private Configuration conf = null;

	private HBaseAdmin hAdmin = null;
	Random rnd = new Random(999999);

	// 每次从mysql中读取多少数据,避免内存溢出
	private final static Integer TABLE_READ_BATCH = 100000;

	private HTable htable = null;

	private Timestamp lastTimestamp;

	public void run() throws Exception {

		init();
		htable = new HTable(conf, TABLE_NAME);
		// ImportContactInOperation operation = new ImportContactInOperation();
		List<String> allTables = readAllMysqlTables();

		for (String tableName : allTables) {
			importTable(tableName);
		}

	}

	/**
	 * 初始化
	 */
	public void init() {
		conf = new Configuration();
		// conf.set("hbase.zookeeper.address", "node06,node08,node09");
		conf.set("hbase.zookeeper.quorum", "node06,node08,node09");
	}

	/**
	 * 按表导入
	 *
	 * @param tableName
	 * @throws Exception
	 */
	private void importTable(String tableName) throws Exception {
		System.out.println("importing " + tableName);
		int userId = Integer.parseInt(tableName.split("operation_")[1]);
		if (userId < 1351)
			return;

		MySqlHelper helper = new MySqlHelper();

		String sql = "select count(1) from " + tableName;
		ResultSet rs = helper.execResultSet(sql);
		int total = 0;
		while (rs.next()) {
			total = rs.getInt(1);
		}

		int totalSize = total / TABLE_READ_BATCH;
		if (total % TABLE_READ_BATCH != 0)
			totalSize++;

		// 考虑到表有可能非常大，按批处理吧
		for (int i = 0; i < totalSize; i++) {

			importTableBatch(tableName, helper, i);

		}

	}

	/**
	 * 按批处理一张表
	 *
	 * @param tableName
	 * @param helper
	 * @throws IOException
	 * @throws SQLException
	 * @throws Exception
	 * @throws InterruptedIOException
	 * @throws RetriesExhaustedWithDetailsException
	 */
	private void importTableBatch(String tableName, MySqlHelper helper, Integer index)
			throws IOException, SQLException, Exception, InterruptedIOException, RetriesExhaustedWithDetailsException {
		int begin = index * TABLE_READ_BATCH;
		String sql = String.format("select * from %s limit %d,%d ", tableName, begin, TABLE_READ_BATCH);
		ResultSet rs = helper.execResultSet(sql);

		HTable htable = new HTable(conf, TABLE_NAME);

		List<Put> puts = new ArrayList<Put>();

		Put put = null;

		String rowKeyPtn = "34_%d_%s";
		String rowKey = null;

		int batchSize = 100;

		int userId = Integer.parseInt(tableName.split("operation_")[1]);

		int sum = 0;
		while (rs.next()) {

			put = buildPut(userId, rs);
			if (put == null)
				continue;
			puts.add(put);

			sum++;

			if (puts.size() > 0 && puts.size() % batchSize == 0) {
				htable.put(puts);
				puts.clear();
				System.out.println("total inserted " + sum);
			}
		}

		if (puts.size() > 0) {
			htable.put(puts);
			puts.clear();
			System.out.println("total inserted " + sum);
		}

		rs.close();
	}

	/**
	 * 从resultset中产生一个put
	 *
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private Put buildPut(int userId, ResultSet rs) throws Exception {
		String rowKeyPtn = "%d_%d_%s";

		int contactId = rs.getInt("contact_id");
		Timestamp timestamp = rs.getTimestamp("create_date");

		Time time = rs.getTime("create_date");

		if (timestamp == null)
			return null;

		Long tk = Long.MAX_VALUE - timestamp.getTime();

		//如果create_date相同，那么把id加上
		if(timestamp.equals(lastTimestamp))
		{
			tk+=rs.getInt("id");
		}

		String rowKey = String.format(rowKeyPtn, userId, contactId, tk);
		lastTimestamp=timestamp;

		Put put = new Put(rowKey.getBytes());
		System.out.println(rowKey);

		// array_type array_element = array[j];
		put = new Put(rowKey.getBytes());

		addToPut(put, rs, "contact_id");
		addToPut(put, rs, "operation_type_id");
		addToPut(put, rs, "operation_id");
		addToPut(put, rs, "description");

		addToPut(put, rs, "status");
		addToPut(put, rs, "after_status");
		addToPut(put, rs, "source_type_id");
		addToPut(put, rs, "source_id");
		addToPut(put, rs, "source");
		// addToPut(put, rs, "create_date");

		put.add("cf".getBytes(), "create_date".getBytes(), String.valueOf(timestamp.getTime()).getBytes("UTF-8"));

		return put;
	}

	/**
	 * 从rs读取指定key的值，如果值不为空，则加到put里
	 *
	 * @param put
	 * @param rs
	 * @param key
	 * @throws Exception
	 */
	public void addToPut(Put put, ResultSet rs, String key) throws Exception {
		String value = rs.getString(key);
		if (value != null)
			put.add("cf".getBytes(), key.getBytes(), value.getBytes("UTF-8"));
	}

	/**
	 * 读取所有表名
	 *
	 * @return
	 * @throws Exception
	 */
	@Test
	public List<String> readAllMysqlTables() throws Exception {
		String sql = "select table_name from information_schema.tables where table_schema='contact_info' and table_name like 'contact_in_operation_%' ";
		MySqlHelper helper = new MySqlHelper();
		ResultSet rs = helper.execResultSet(sql);

		List<String> tables = new ArrayList<String>();

		// 展开结果集数据库
		while (rs.next()) {

			String tableName = rs.getString("table_name");
			tables.add(tableName);
		}
		rs.close();

		return tables;

	}

	/**
	 * 联系人行为轨迹表插入测试
	 *
	 * @throws Exception
	 */
	@Test
	public void insert() throws Exception {
		HTable htable = new HTable(conf, TABLE_NAME);

		List<Put> puts = new ArrayList<Put>();

		Put put = null;

		String rowKeyPtn = "34_%d_%s";
		String rowKey = null;

		int batchSize = 100;

		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < batchSize; j++) {
				Long tk = Long.MAX_VALUE - createDate(2019);
				rowKey = String.format(rowKeyPtn, i, tk);

				System.out.println(rowKey);

				// array_type array_element = array[j];
				put = new Put(rowKey.getBytes());

				Integer type = j % 2 == 0 ? 20 : 50;
				put.add("cf".getBytes(), "type".getBytes(), String.valueOf(type).getBytes("UTF-8"));

				// contact_id已经在rowkey里了
				// put.add("cf".getBytes(), "contact_id".getBytes(),
				// "1762".getBytes("UTF-8"));

				put.add("cf".getBytes(), "operation_type_id".getBytes(), "1762".getBytes("UTF-8"));

				put.add("cf".getBytes(), "operation_id".getBytes(), "1762".getBytes("UTF-8"));
				put.add("cf".getBytes(), "description".getBytes(),
						("我爱你中国sdfsdfsdaa3sdsd" + rnd.nextInt()).getBytes("UTF-8"));
				put.add("cf".getBytes(), "status".getBytes(), "1".getBytes("UTF-8"));

				put.add("cf".getBytes(), "after_status".getBytes(), "1".getBytes("UTF-8"));

				put.add("cf".getBytes(), "source_type_id".getBytes(), "1762".getBytes("UTF-8"));

				Integer sourceId = j % 3 == 0 ? 2032 : 3389;
				put.add("cf".getBytes(), "source_id".getBytes(), String.valueOf(sourceId).getBytes("UTF-8"));

				put.add("cf".getBytes(), "source".getBytes(), "手机号".getBytes("UTF-8"));
				puts.add(put);

				put.add("cf".getBytes(), "create_date".getBytes(), String.valueOf((new Date()).getTime()).getBytes());
				puts.add(put);
			}

			htable.put(puts);
			puts.clear();
			System.out.println("processeed " + i * batchSize);
		}
	}

	/**
	 * 按前缀查询，性能较差
	 * @throws Exception
	 */
	@Test
	public void searchByPrefix() throws Exception {
		HTable htable = new HTable(conf, TABLE_NAME);

		Scan scan = new Scan();
		FilterList filters = new FilterList(Operator.MUST_PASS_ALL);

		Filter filter = new PrefixFilter("34_985".getBytes());
		filters.addFilter(filter);

		scan.setFilter(filters);
		ResultScanner scanner = htable.getScanner(scan);

		for (Result result : scanner) {
			printRow(result);
		}

	}

	/**
	 * 按前缀和类型来查
	 *
	 * @throws Exception
	 */
	@Test
	public void searchByPrefixAndType() throws Exception {
		HTable htable = new HTable(conf, TABLE_NAME);

		Scan scan = new Scan();
		FilterList filters = new FilterList(Operator.MUST_PASS_ALL);

		// Filter filter = new PrefixFilter("10".getBytes());
		// filters.addFilter(filter);

		// Filter filter2 = new SingleColumnValueFilter("cf".getBytes(),
		// "operation_type_id".getBytes(), CompareOp.EQUAL,
		// "52".getBytes());
		// filters.addFilter(filter2);

		Filter filter3 = new SingleColumnValueFilter("cf".getBytes(), "contact_id".getBytes(), CompareOp.EQUAL,
				"73".getBytes());
		filters.addFilter(filter3);

		scan.setFilter(filters);
		ResultScanner scanner = htable.getScanner(scan);

		int sum = 0;
		for (Result result : scanner) {
			printRow(result);
			sum++;
		}

		System.out.println("total is " + sum);

	}

	/**
	 * 打印一行
	 *
	 * @param result
	 */
	private void printRow(Result result) {

		Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "operation_id".getBytes());
		System.out.println(Bytes.toString(CellUtil.cloneValue(cell1)));

		Cell cell3 = result.getColumnLatestCell("cf".getBytes(), "source".getBytes());
		System.out.println(Bytes.toString(CellUtil.cloneValue(cell3)));

		Cell cell4 = result.getColumnLatestCell("cf".getBytes(), "source_id".getBytes());
		if (cell4 != null)
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell4)));

		Cell cell5 = result.getColumnLatestCell("cf".getBytes(), "status".getBytes());
		if (cell5 != null)
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell5)));

		Cell cell6 = result.getColumnLatestCell("cf".getBytes(), "type".getBytes());
		if (cell6 != null)
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell6)));

		Cell cell7;
		try {
			cell7 = result.getColumnLatestCell("cf".getBytes(), "description".getBytes("UTF-8"));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell7)));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public long createDate(int year) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		Date date = sdf.parse(String.format("%04d%02d%02d%02d%02d%02d", year, rnd.nextInt(12) + 1, rnd.nextInt(31) + 1,
				rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60)));
		return date.getTime();
	}

	@After
	public void destroy() {

	}

}
