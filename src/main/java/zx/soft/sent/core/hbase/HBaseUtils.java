package zx.soft.sent.core.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.config.ConfigUtil;

public class HBaseUtils {
	private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

	private static Configuration conf = null;
	private static HConnection conn = null;

	static {
		Properties properties = ConfigUtil.getProps("hbase.properties");
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"));
		conf.setInt("hbase.zookeeper.property.clientPort",
				Integer.parseInt(properties.getProperty("hbase.zookeeper.property.clientPort")));
		try {
			conn = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	public static void createTable(String tableName, String[] families) throws MasterNotRunningException,
	ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			logger.info("table" + tableName + "already exists!");
		} else {
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for (String family : families) {
				descriptor.addFamily(new HColumnDescriptor(family));
			}
			admin.createTable(descriptor);
			logger.info("create table " + tableName + " success!");
		}
		admin.close();

	}

	public static boolean addSingleColumn(String tableName, String rowKey, String family, String qualifier, String value) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Put put = new Put(Bytes.toBytes(rowKey));
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				table.put(put);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean put(String tableName, Put put) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				table.put(put);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean put(String tableName, List<Put> put) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				table.put(put);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean appendColumn(String tableName, String rowKey, String family, String qualifier, String value) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Append append = new Append(Bytes.toBytes(rowKey));
				append.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				table.append(append);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean incrementColumn(String tableName, String rowKey, String family, String qualifier, long value) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Increment incre = new Increment(Bytes.toBytes(rowKey));
				incre.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
				table.increment(incre);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static Cell getOneColumn(String tableName, String rowKey, String family, String qualifier) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Get get = new Get(Bytes.toBytes(rowKey));
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				Result result = table.get(get);
				return result.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static boolean scanRows(String tableName, String startRow, String stopRow) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
				ResultScanner scanner = table.getScanner(scan);
				for (Result result : scanner) {
					for (Cell cell : result.listCells()) {
						System.out.println(new String(CellUtil.cloneRow(cell)));
					}

				}
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean scanByFilter(String tableName, String rowKey, String family, String qualifier, String value) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Scan scan = new Scan();
				scan.addColumn(family.getBytes(), qualifier.getBytes());
				Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier),
						CompareOp.EQUAL, Bytes.toBytes(value));
				scan.setFilter(filter);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean deleteRow(String tableName, String rowKey) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Delete delete = new Delete(Bytes.toBytes(rowKey));
				table.delete(delete);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean deleteColumn(String tableName, String rowKey, String family, String qualifier) {
		try {
			HTableInterface table = null;
			try {
				table = conn.getTable(tableName);
				Delete delete = new Delete(Bytes.toBytes(rowKey));
				delete.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				table.delete(delete);
				return true;
			} finally {
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void close() {
		try {
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseUtils.createTable("test", new String[] { "a", "b" });
	}
}
