package com.test.dbunit.common.test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mockit.Delegate;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.filter.IColumnFilter;
import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.test.dbunit.common.db.DbConnection;
import com.test.dbunit.common.db.TestConnection;
import com.test.dbunit.common.util.DbUnitUtils;
import com.test.dbunit.common.util.LibraryHack;
import com.test.dbunit.common.util.StaticColumnFilter;
import com.test.dbunit.common.util.StaticColumnFilter.PrimaryKey;

/**
 * JUnitを記述する際の基底クラス。
 */
public abstract class DBTestBase {

	private static Log log = LogFactory.getLog(DBTestBase.class);

	@Rule
	public TestName name = new TestName();

	/**
	 * テストに利用するコネクション。<br/>
	 * このコネクションは{@link Connection#commit()}がコールされた場合でもコミットされず、トランザクションが継続します。
	 * 原則として、このコネクションを利用してください。<br/>
	 * {@link Before}で確保され、{@link After}で解放されます。<br/>
	 * このコネクション以外を利用したい場合、{@link #newConnection()}で新しいコネクションを取得してください。<br/>
	 * 新しいコネクションを取得した場合は、必ず{@link #releaseConnection(Connection)}で解放してください。
	 * @see #getRollbackDisabledConnection()
	 */
	protected Connection conn = null;

	/**
	 * テストに利用するコネクション。<br/>
	 * コミットとロールバックを無効にしています。<br/>
	 * このコネクションは{@link Connection#rollback()}がコールされた場合でもロールバックされません。<br/>
	 * コミットを無効にしている為、ロールバックのテストが必要な場合などに利用してください。<br/>
	 * ライフサイクルやトランザクションについては{@link #conn}と同等となります。
	 * @see #conn
	 * @see #getRollbackDisabledConnection()
	 */
	private Connection rollbackDisabledConn = null;

	/**
	 * DBUnitを利用する為のコネクション。<br/>
	 * ライフサイクルは{@link #conn}と同じです。
	 */
	protected IDatabaseConnection dbunitConn = null;

	/**
	 *
	 */
	@BeforeClass
	public static void beforeBaseClass() {

		log.info("DBTestBase: @BeforeClass end.");
	}

	/**
	 *
	 */
	@AfterClass
	public static void afterBaseClass() {
		log.info("DBTestBase: @AfterClass end.");
	}

	/**
	 *
	 * @throws Exception
	 */
	@Before
	public void beforeBase() throws Exception {
		// コミットできないようにTestConnectionのメソッドを差し替えておく
		new NonStrictExpectations() {
			@Mocked(methods = { "commit" })
			TestConnection testConnection;
			{
				testConnection.commit();
				result = new Delegate<TestConnection>() {
					@SuppressWarnings("unused")
					private void commit() {
						log.info("(Called mock commit(). Nothing to do.)");
					}
				};
			}
		};

		// ライブラリハック
		LibraryHack.mockup();

		// テスト用コネクション確保
		setupConnections();
		log.info("DBTestBase: @Before end.");
	}

	/**
	 *
	 * @throws Exception
	 */
	@After
	public void afterBase() throws Exception {
		// テスト用コネクション解放
		if ((conn != null) && !conn.isClosed()) {
			DbUtils.rollbackAndCloseQuietly(conn);
		}
		log.info("DBTestBase: @After end.");
	}

	/**
	 * 新しいコネクションを取得します。<br/>
	 * このコネクションは{@link Connection#commit()}がコールされた場合でもコミットされず、トランザクションが継続します。
	 * 新しいコネクションを取得した場合は、必ず{@link #releaseConnection(Connection)}で解放してください。
	 * @throws Exception
	 */
	private void setupConnections() throws Exception {
		Connection testConn = DbConnection.getConnection();
		// commit のみ無効
		this.conn = testConn;
		// このインスタンスのrollback無効。中身はメインコネクションと同じ
		this.rollbackDisabledConn = testConn;
		new NonStrictExpectations(rollbackDisabledConn) {
			{
				rollbackDisabledConn.rollback();
				result = new Delegate<TestConnection>() {
					@SuppressWarnings("unused")
					private void rollback() throws SQLException {
						log.info("(Called mock rollback(). Nothing to do.)");
					}
				};
			}
		};
		this.dbunitConn = DbUnitUtils.getDbUnitConnection(conn, DbConnection.getSchema());
	}

	/**
	 * 新しいコネクションを取得します。<br/>
	 * このコネクションは{@link Connection#commit()}がコールされた場合でもコミットされず、トランザクションが継続します。
	 * 新しいコネクションを取得した場合は、必ず{@link #releaseConnection(Connection)}で解放してください。
	 * @return	コネクション
	 * @throws Exception
	 */
	protected Connection newConnection() throws Exception {
		return DbConnection.getConnection();
	}

	/**
	 * コネクションを解放します。
	 * @param conn	コネクション
	 */
	protected void releaseConnection(Connection conn) {
		DbUtils.rollbackAndCloseQuietly(conn);
	}

	/**
	 * コミットとロールバックを無効にしたコネクションを取得します。<br/>
	 * このコネクションは{@link #conn}と同じトランザクションになりますが、
	 * このメソッドで取得したコネクションのみ、ロールバックが無効になります。<br/>
	 * 呼び出し側で<code>{@link #conn}.rollback()</code>をコールすると、このコネクションのトランクザクションをロールバックすることが可能です。
	 *
	 * @return ロールバックを無効にしたコネクション
	 */
	protected Connection getRollbackDisabledConnection() {
		return rollbackDisabledConn;
	}

	/**
	 * ファイル名からFileクラスを返します。<br/>
	 * ファイルは利用するクラスと同階層の「resources」パッケージ内に存在する必要があります。<br/>
	 * jp.co.worksap.settle.SettleTest から下記のように呼び出した場合、<br/>
	 * ファイルは「jp.co.worksap.settle.resources.test.xls」にあるとみなされます。
	 * <pre>
	 * getResourceFile("test.xls"); // リソースがresources直下にある場合
	 * getResourceFile("foo/test.xls"); // リソースがresources/fooにある場合
	 * </pre>
	 * @param fileName
	 * @return File
	 * @throws Exception
	 */
	protected File getResourceFile(String fileName) throws Exception {

		StringBuilder resourcePathBuff = new StringBuilder();
		resourcePathBuff.append(this.getClass().getPackage().getName().replaceAll("\\.", "/"));
		resourcePathBuff.append("/resources/");
		resourcePathBuff.append(fileName);

		URL url = this.getClass().getClassLoader().getResource(resourcePathBuff.toString());
		if (url == null) {
			throw new Exception(resourcePathBuff.toString() + " is not found or unauthorized.");
		}

		File file;
		try {
			file = new File(url.toURI());
		} catch (URISyntaxException e) {
			throw new Exception(url.toString() + " is not found.", e);
		}
		if (!file.exists()) {
			throw new Exception(file.toString() + " is not found.");
		}
		return file;
	}

	/**
	 * ファイルからデータベースにデータを挿入します。<br/>
	 * ファイルの指定方法は{@link #loadResourceFile(String)}の説明を参考にしてください。
	 *
	 * @param fileName
	 * @throws Exception
	 * @see #loadResourceFile(String)
	 * @see #insertFrom(String, PrimaryKey...)
	 * @return 挿入した{@link IDataSet}
	 */
	protected IDataSet insertFrom(String fileName) throws Exception {
		return insertFrom(fileName, (PrimaryKey[])null);
	}

	/**
	 * ファイルからデータベースにデータを挿入します。<br/>
	 * このメソッドは静的にプライマリーキーを指定する為、{@link #insertFrom(String)}よりも高速に動作します。
	 * ファイルの指定方法は{@link #loadResourceFile(String)}の説明を参考にしてください。
	 *
	 * @param fileName
	 * @param primaryKeys
	 * @throws Exception
	 * @return 挿入した{@link IDataSet}
	 */
	protected IDataSet insertFrom(String fileName, PrimaryKey... primaryKeys) throws Exception {
		return dbOperate(DatabaseOperation.INSERT, fileName, primaryKeys);
	}

	/**
	 * ファイルからデータベースにデータを挿入します。<br/>
	 * このメソッドは静的にプライマリーキーを指定する為、{@link #insertFrom(String)}よりも高速に動作します。
	 * ファイルの指定方法は{@link #loadResourceFile(String)}の説明を参考にしてください。
	 *
	 * @param fileName
	 * @param primaryKeys
	 * @throws Exception
	 * @return 挿入した{@link IDataSet}
	 */
	protected IDataSet refresh(String fileName, PrimaryKey... primaryKeys) throws Exception {
		return dbOperate(DatabaseOperation.REFRESH, fileName, primaryKeys);
	}

	/**
	 * DB操作
	 * @param operation		DB操作
	 * @param fileName		ファイル名
	 * @param primaryKeys
	 * @return	Dataset
	 * @throws Exception
	 */
	private IDataSet dbOperate(DatabaseOperation operation, String fileName, PrimaryKey... primaryKeys)
			throws Exception {

		StaticColumnFilter filter = null;
		if ((primaryKeys != null) && (primaryKeys.length > 0)) {
			filter = new StaticColumnFilter(primaryKeys);
		}

		DatabaseConfig config = dbunitConn.getConfig();
		IColumnFilter orginalFilter = null;
		if (filter != null) {
			orginalFilter = (IColumnFilter)config.getProperty(DatabaseConfig.PROPERTY_PRIMARY_KEY_FILTER);
			config.setProperty(DatabaseConfig.PROPERTY_PRIMARY_KEY_FILTER, filter);
		}
		config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());

		try {
			IDataSet ds = loadResourceFile(fileName);
			operation.execute(dbunitConn, ds);
			return ds;
		} finally {
			if (filter != null) {
				config.setProperty(DatabaseConfig.PROPERTY_PRIMARY_KEY_FILTER, orginalFilter);
			}
		}
	}

	/**
	 * SQLによるレコード取得
	 * @param tableName	テーブル名
	 * @param sql		SQL
	 * @return	レコード（MAP）リスト
	 * @throws Exception
	 */
	protected List<Map<String, Object>> getRecordsFromSql(String tableName, String sql) throws Exception {

		List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

		ITable table = dbunitConn.createQueryTable(tableName, sql);

		ITableMetaData tableMetaData = table.getTableMetaData();
		int rowCount = table.getRowCount();

		for (int i = 0; i < rowCount; i++) {

			Map<String, Object> record = new HashMap<String, Object>();

			for (Column column : tableMetaData.getColumns()) {
				String key = column.getColumnName();
				Object value = table.getValue(i, key);
				record.put(key, value);
			}

			records.add(record);
		}

		return records;
	}

	/**
	 * IDataSetからレコード取得
	 * @param dataset	データセット
	 * @param tableName	テーブル名
	 * @return	レコード（MAP）リスト
	 * @throws Exception
	 */
	protected List<Map<String, Object>> getRecordsFromIDataSet(IDataSet dataset, String tableName) throws Exception {

		List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

		String[] colNames = getColsFromDataSet(dataset, tableName);

		ITable table = dataset.getTable(tableName);

		int rowCount = table.getRowCount();

		for (int i = 0; i < rowCount; i++) {

			Map<String, Object> record = new HashMap<String, Object>();

			for (String colName : colNames) {
				Object value = table.getValue(i, colName);
				record.put(colName, value);
			}

			records.add(record);
		}

		return records;
	}

	/**
	 * 期待値データセット取得
	 * @param testMethod	テストメソッド名
	 * @return	データセット
	 * @throws Exception
	 */
	protected IDataSet getExpectedDataset(String testMethod) throws Exception {
		// 結果検証
		String resourceFolder = getResourceFolder(testMethod);

		// その他は結果データExcelから
		String fileName = resourceFolder + "/expected.xls";

		return (ReplacementDataSet)loadResourceFile(fileName);
	}

	/**
	 * 投入データをIDataSetに変換<br>
	 * 置換文字列に対応するデータ型を指定する。
	 * @param fileName	投入データのファイル名
	 * @return	投入DataSet
	 * @throws Exception
	 */
	protected IDataSet loadResourceFile(String fileName) throws Exception {

		File file = getResourceFile(fileName);

		Map<Object, Object> replacements = new HashMap<Object, Object>();
		setReplacement(replacements);

		return DbUnitUtils.newDataSet(file, replacements);
	}

	/**
	 * 置換文字列に対応するデータ型を指定する。
	 * @param replacements	置換ルールMap
	 */
	protected void setReplacement(Map<Object, Object> replacements) {

		Date today = new Date();
		Date truncToday = DateUtils.truncate(today, Calendar.DAY_OF_MONTH);
		Date yesterDay = DateUtils.addDays(today, -1);
		Date truncYesterDay = DateUtils.truncate(yesterDay, Calendar.DAY_OF_MONTH);
		Date daysBefore3 = DateUtils.addDays(new Date(), -3);
		Date truncDaysBefore3 = DateUtils.truncate(daysBefore3, Calendar.DAY_OF_MONTH);

		replacements.put("[NULL]", null);
		replacements.put("[BLANK]", "");
		replacements.put("[NOW]", new Date(System.currentTimeMillis()));
		replacements.put("[TODAY]", truncToday);
		replacements.put("[YESTERDAY]", truncYesterDay);
		replacements.put("[3DAYSBEFORE]", truncDaysBefore3);
		replacements.put("[SYSDATE]", new Timestamp(System.currentTimeMillis()));
		replacements.put("[PREDAY]", DateFormatUtils.format(yesterDay, "yyyyMMdd"));
		replacements.put("[YYYYMMDD]", DateFormatUtils.format(new Date(), "yyyyMMdd"));

		addReplacement(replacements);
	}

	/**
	 * 置換文字列に対応するデータ型を追加指定する。<br>
	 * 具体化は個別テストクラスにて
	 * @param replacements	置換ルールMap
	 */
	protected void addReplacement(Map<Object, Object> replacements) {
	}

	/**
	 * 期待値Datasetから検証対象カラムを取得する
	 * @param dataset	期待値
	 * @param tableName	テーブル名
	 * @return	検証対象カラム
	 * @throws Exception
	 */
	private String[] getColsFromDataSet(IDataSet dataset, String tableName) throws Exception {

		ITableMetaData tableMetaData = dataset.getTableMetaData(tableName);

		Column[] columns = tableMetaData.getColumns();
		String[] colNames = new String[columns.length];

		for (int i = 0; i < columns.length; i++) {
			Column column = columns[i];
			colNames[i] = column.getColumnName();
		}

		return colNames;
	}

	/**
	 * リソースフォルダを取得する。<br>
	 * "クラス名／ユーザID"
	 * @param testMethod
	 * @return	リソースフォルダ
	 */
	protected String getResourceFolder(String testMethod) {
		String className = this.getClass().getSimpleName();
		return className + "/" + testMethod;
	}

	/**
	 * 準備作業<br>
	 * 準備データを投入する
	 * @param testMethod
	 * @param pks	primary keys
	 * @throws Throwable
	 */
	protected void prepare(String testMethod, PrimaryKey... pks) throws Throwable {

		String resourceFolder = getResourceFolder(testMethod);

		// データ投入
		refresh(resourceFolder + "/input.xls");
	}

	/**
	 * テーブルを検証
	 * @param dataset
	 * @param sql
	 * @param tableName
	 * @param ignoreColumns
	 * @throws Exception
	 */
	protected void assertTable(IDataSet dataset, String sql, String tableName, String... ignoreColumns)
			throws Exception {
		Assertion.assertEqualsByQuery(dataset, dbunitConn, sql, tableName, ignoreColumns);
	}
}
