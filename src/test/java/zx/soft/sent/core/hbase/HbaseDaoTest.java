package zx.soft.sent.core.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.Before;
import org.junit.Test;

public class HbaseDaoTest {

	@Before
	public void setup() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseUtils.createTable("tdh_fdlawcase", new String[] { "info", "meta" });
	}

	@Test
	public void test() {
		HbaseDao dao = HbaseDao.getInstance("tdh_fdlawcase");
		dao.addSingleColumn("F7508ADF9B0611E595270CDA411DB18D", "info", "info_个人赔付比例", "1.0");
		dao.addSingleColumn("F7508ADF9B0611E595270CDA411DB18D", "info", "info_就医范围", "市内医院");
		dao.addSingleColumn("F7508ADF9B0611E595270CDA411DB18D", "info", "info_转让拼装报废", "false");
		dao.addSingleColumn("F7508ADF9B0611E595270CDA411DB18D", "info", "info_索要赔偿金额", "130254.75");
		dao.addSingleColumn("F7508ADF9B0611E595270CDA411DB18D", "info", "info_受害总人数", "3");
		dao.addSingleColumn("F7508ADF9B0611E595270CDA411DB18D", "meta", "meta_当事人关系",
						"[{\"sourcePersonName\":\"阳光财产保险股份有限公司芜湖中心支公司\",\"targetPersonName\":\"荣发厚\",\"relations\":[\"法定代表人\"]},{\"sourcePersonName\":\"阳光财产保险股份有限公司芜湖中心支公司\",\"targetPersonName\":\"黄磊\",\"relations\":[\"委托代理人\"]}]");
		dao.stop();
		HBaseUtils.close();

	}

}
