package com.test.dbunit.sample.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbunit.dataset.IDataSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.test.dbunit.common.db.DbExecuter;
import com.test.dbunit.common.db.SQLs;
import com.test.dbunit.common.test.DBTestBase;
import com.test.dbunit.common.util.StaticColumnFilter.PrimaryKey;

/**
 *
 *
 *
 */
public class SampleDBTest extends DBTestBase {

	private static Log log = LogFactory.getLog(SampleDBTest.class);

	/** テーブル名 */
	private static final String T_TEST_001 = "TEST_001";

	/** テーブル名 */
	private static PrimaryKey PKEYS[] = { new PrimaryKey(T_TEST_001, new String[] { "TEST_ID" }) };

	/**
	 *
	 * @throws Exception
	 */
	@BeforeClass
	public static void createTable() throws Exception {
		DbExecuter.update("CREATE_TEST_001"); // sql.properties参照
	}

	/**
	 *
	 * @throws Exception
	 */
	@AfterClass
	public static void dropTable() throws Exception {
		DbExecuter.update("DROP_TEST_001"); // sql.properties参照
	}

	/**
	 *
	 * @throws Throwable
	 */
	@Test
	public void test() throws Throwable {

		String testMethod = name.getMethodName();

		// 準備データinsert
		this.prepare(testMethod, PKEYS);

		// Business Logic 実行

		// 結果検証
		IDataSet dataset = getExpectedDataset(testMethod);

		//
		String sql = SQLs.getSql("ASSERT_TEST_001");
		String[] ignoreColumns = new String[] {};
		this.assertTable(dataset, sql, T_TEST_001, ignoreColumns);
	}
}
