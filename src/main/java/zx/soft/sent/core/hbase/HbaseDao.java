package zx.soft.sent.core.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.log.LogbackUtil;

/**
 * Hbase Dao
 * @author donglei
 * @date: 2016年5月16日 下午10:05:01
 */
public class HbaseDao implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(HbaseDao.class);

	private List<Put> puts;
	private String tableName;

	private AtomicBoolean running = new AtomicBoolean(true);

	private static Map<String, HbaseDao> instances = new ConcurrentHashMap<>();


	private HbaseDao(String tableName, String[] families) {
		try {
			HBaseUtils.createTable(tableName, families);
			this.tableName = tableName;
			this.puts = new ArrayList<Put>();
			new Thread(this).start();
		} catch (IOException e) {
			logger.error(LogbackUtil.expection2Str(e));
		}
	}

	public static HbaseDao getInstance(String table, String[] familys) {
		if (!instances.containsKey(table)) {
			instances.put(table, new HbaseDao(table, familys));
		}
		return instances.get(table);
	}


	public void addSingleColumn(String rowKey, String family, String qualifier, String value) {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		synchronized (puts) {
			puts.add(put);
			if (puts.size() >= 500) {
				flushPuts();
			}
		}
	}

	public void addSingleColumn(byte[] rowKey, String family, String qualifier, String value) {
		Put put = new Put(rowKey);
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		synchronized (puts) {
			puts.add(put);
			if (puts.size() >= 500) {
				flushPuts();
			}
		}
	}

	private void flushPuts() {
		synchronized (puts) {
			HBaseUtils.put(tableName, puts);
			puts.clear();
		}
	}

	@Override
	public void run() {
		while (this.running.get()) {
			try {
				TimeUnit.MINUTES.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			flushPuts();
		}
	}

	public void stop() {
		this.running.set(false);
	}
}
