package com.steven.hbase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.CharSet;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
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

import com.google.common.base.Stopwatch;

public class ContactInOperationTest {

	private final static String TABLE_NAME = "contact_in_operation_all";

	private Configuration conf = null;

	private HBaseAdmin hAdmin = null;
	Random rnd = new Random(999999);

	@Before
	public void init() {
		conf = new Configuration();
		// conf.set("hbase.zookeeper.address", "node06,node08,node09");
		conf.set("hbase.zookeeper.quorum", "node06,node08,node09");

		Date date2 = new Date(1571809352452L);
		System.out.println(date2);
	}

	/**
	 * 联系人行为轨迹表插入测试
	 *
	 * @throws Exception
	 */
	@Test
	public void insertTest() throws Exception {
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
	 * 按rowkey获取
	 * @param rowkey
	 * @throws Exception
	 */

	public void searchByRowkey(String rowkey) throws Exception {
		HTable htable = new HTable(conf, TABLE_NAME);

		Scan scan = new Scan();

		Get get=new Get(rowkey.getBytes());
		Result result= htable.get(get);

			printRowKey(result);
			printRow(result);

	}


	/**
	 * 按前缀查询，性能较差 ，如按 1362_3521_，需cost 30383 ms
	 * @throws Exception
	 */
	@Test
	public void searchByPrefix() throws Exception {

		FilterList filters = new FilterList(Operator.MUST_PASS_ALL);

		Filter filter = new PrefixFilter("1362_3521_".getBytes());
		filters.addFilter(filter);

		search("","",filters);

	}

	/**
	 * 按start_row,stop_row查，性能较好,如按 1362_3521_，需cost  200  ms,两者相差将近100倍
	 * 所以后续，凡是能确定的，都按start_row,stop_row
	 * @throws Exception
	 */
	@Test
	public void searchByStartStopRow() throws Exception {
		HTable htable = new HTable(conf, TABLE_NAME);

		String prefix="1362_3521_";
		Date beginDate=new Date(100,1,1);
		Date endDate=new Date(120,1,1);

		String startRow=prefix+( Long.MAX_VALUE-endDate.getTime());
		String stopRow=prefix+( Long.MAX_VALUE-beginDate.getTime());

		Scan scan = new Scan();
//		FilterList filters = new FilterList(Operator.MUST_PASS_ALL);
//
//		Filter filter = new PrefixFilter("1362_3521".getBytes());
//		filters.addFilter(filter);

		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
//		scan.setFilter(filters);
		Stopwatch sw=new Stopwatch();
		sw.start();

		ResultScanner scanner = htable.getScanner(scan);


		for (Result result : scanner) {
			printRowKey(result);
			printRow(result);
		}

		sw.stop();
		System.out.printf("cost %s ms",sw.elapsedTime(TimeUnit.MILLISECONDS));

	}

	/**
	 * 既有start_row,stop_row,又有其他条件的查询
	 * @throws Exception
	 */
	@Test
	public void searchByRowAndFilter() throws Exception {

		String start_prefix="1362_1_";
		Date startDate=new Date(100,1,1);

		String stop_prefix="1362_99999999_";
		Date stopDate=new Date(120,1,1);

		String startRow=start_prefix+( Long.MAX_VALUE-stopDate.getTime());
		String stopRow=stop_prefix+( Long.MAX_VALUE-startDate.getTime());

		FilterList filters = new FilterList(Operator.MUST_PASS_ALL);

		Filter filter2=new SingleColumnValueFilter("cf".getBytes()	, "operation_type_id".getBytes(), CompareOp.EQUAL, "1".getBytes());
		filters.addFilter(filter2);

		search(startRow,stopRow,filters);

	}

	/**
	 * 按前缀和类型来查
	 *
	 * @throws Exception
	 */
	@Test
	public void searchByPrefixAndType() throws Exception {
		FilterList filters = new FilterList(Operator.MUST_PASS_ALL);

		Filter filter = new PrefixFilter("34_".getBytes());
		filters.addFilter(filter);

		Filter filter2 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(), CompareOp.EQUAL,
				"20".getBytes());
		filters.addFilter(filter2);

		search("","",filters);
	}

	/**
	 * 按前缀查询，性能较差 ，如按 1362_3521_，需cost 30383 ms
	 * @throws Exception
	 */
	@Test
	public void deleteRow() throws Exception {
		HTable htable = new HTable(conf, TABLE_NAME);

		String rowKey="1362_3521_9223370499502847703";

		searchByRowkey(rowKey);

		Delete delete=new Delete("1362_3521_9223370499502847703".getBytes());
		htable.delete(delete);

		searchByRowkey(rowKey);

	}

	/**
	 * 打印rowkey
	 *
	 * @param result
	 */
	private void printRowKey(Result result) {
		String rowKey = "";
		for (KeyValue kv : result.raw()) {
			int rowlength = Bytes.toShort(kv.getBuffer(), kv.getOffset() + KeyValue.ROW_OFFSET);
			rowKey = Bytes.toStringBinary(kv.getBuffer(), kv.getOffset() + KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT,
					rowlength);
			if (!StringUtils.isBlank(rowKey)) {
				System.out.println("----------------------");
				System.out.println(rowKey);
				break;
			}
		}
	}

	/**
	 * 打印一行
	 *
	 * @param result
	 */
	private void printRow(Result result) {

		Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "operation_id".getBytes());
		if (cell1 != null)
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell1)));

		Cell cell3 = result.getColumnLatestCell("cf".getBytes(), "source".getBytes());
		if (cell3 != null)
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Cell cell8 = result.getColumnLatestCell("cf".getBytes(), "create_date".getBytes());
		if (cell8 != null) {
			long ticket = Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell8)));
			Date date = new Date(ticket);
			System.out.println(date);
		}
	}

	public long createDate(int year) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		Date date = sdf.parse(String.format("%04d%02d%02d%02d%02d%02d", year, rnd.nextInt(12) + 1, rnd.nextInt(31) + 1,
				rnd.nextInt(24), rnd.nextInt(60), rnd.nextInt(60)));
		return date.getTime();
	}


	/**
	 * 最全的方法,包括startRow,stopRow,filter
	 * @param startRow
	 * @param stopRow
	 * @param filters
	 * @throws Exception
	 */
	public void search(String startRow,String stopRow,FilterList filters) throws Exception {
		HTable htable = new HTable(conf, TABLE_NAME);

		Scan scan = new Scan();
		if(filters!=null)
			scan.setFilter(filters);

		if(!StringUtils.isEmpty(startRow))
			scan.setStartRow(startRow.getBytes());

		if(!StringUtils.isEmpty(stopRow))
			scan.setStopRow(stopRow.getBytes());
//		scan.setFilter(filters);
		Stopwatch sw=new Stopwatch();
		sw.start();

		ResultScanner scanner = htable.getScanner(scan);

		int sum=0;
		for (Result result : scanner) {
			printRowKey(result);
			printRow(result);
			sum++;
		}

		sw.stop();
		System.out.printf("cost %s ms,total %d",sw.elapsedTime(TimeUnit.MILLISECONDS),sum);

	}

	@After
	public void destroy() {

	}

}
