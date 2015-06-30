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
 * JUnit���L�q����ۂ̊��N���X�B
 */
public abstract class DBTestBase {

	private static Log log = LogFactory.getLog(DBTestBase.class);

	@Rule
	public TestName name = new TestName();

	/**
	 * �e�X�g�ɗ��p����R�l�N�V�����B<br/>
	 * ���̃R�l�N�V������{@link Connection#commit()}���R�[�����ꂽ�ꍇ�ł��R�~�b�g���ꂸ�A�g�����U�N�V�������p�����܂��B
	 * �����Ƃ��āA���̃R�l�N�V�����𗘗p���Ă��������B<br/>
	 * {@link Before}�Ŋm�ۂ���A{@link After}�ŉ������܂��B<br/>
	 * ���̃R�l�N�V�����ȊO�𗘗p�������ꍇ�A{@link #newConnection()}�ŐV�����R�l�N�V�������擾���Ă��������B<br/>
	 * �V�����R�l�N�V�������擾�����ꍇ�́A�K��{@link #releaseConnection(Connection)}�ŉ�����Ă��������B
	 * @see #getRollbackDisabledConnection()
	 */
	protected Connection conn = null;

	/**
	 * �e�X�g�ɗ��p����R�l�N�V�����B<br/>
	 * �R�~�b�g�ƃ��[���o�b�N�𖳌��ɂ��Ă��܂��B<br/>
	 * ���̃R�l�N�V������{@link Connection#rollback()}���R�[�����ꂽ�ꍇ�ł����[���o�b�N����܂���B<br/>
	 * �R�~�b�g�𖳌��ɂ��Ă���ׁA���[���o�b�N�̃e�X�g���K�v�ȏꍇ�Ȃǂɗ��p���Ă��������B<br/>
	 * ���C�t�T�C�N����g�����U�N�V�����ɂ��Ă�{@link #conn}�Ɠ����ƂȂ�܂��B
	 * @see #conn
	 * @see #getRollbackDisabledConnection()
	 */
	private Connection rollbackDisabledConn = null;

	/**
	 * DBUnit�𗘗p����ׂ̃R�l�N�V�����B<br/>
	 * ���C�t�T�C�N����{@link #conn}�Ɠ����ł��B
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
		// �R�~�b�g�ł��Ȃ��悤��TestConnection�̃��\�b�h�������ւ��Ă���
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

		// ���C�u�����n�b�N
		LibraryHack.mockup();

		// �e�X�g�p�R�l�N�V�����m��
		setupConnections();
		log.info("DBTestBase: @Before end.");
	}

	/**
	 *
	 * @throws Exception
	 */
	@After
	public void afterBase() throws Exception {
		// �e�X�g�p�R�l�N�V�������
		if ((conn != null) && !conn.isClosed()) {
			DbUtils.rollbackAndCloseQuietly(conn);
		}
		log.info("DBTestBase: @After end.");
	}

	/**
	 * �V�����R�l�N�V�������擾���܂��B<br/>
	 * ���̃R�l�N�V������{@link Connection#commit()}���R�[�����ꂽ�ꍇ�ł��R�~�b�g���ꂸ�A�g�����U�N�V�������p�����܂��B
	 * �V�����R�l�N�V�������擾�����ꍇ�́A�K��{@link #releaseConnection(Connection)}�ŉ�����Ă��������B
	 * @throws Exception
	 */
	private void setupConnections() throws Exception {
		Connection testConn = DbConnection.getConnection();
		// commit �̂ݖ���
		this.conn = testConn;
		// ���̃C���X�^���X��rollback�����B���g�̓��C���R�l�N�V�����Ɠ���
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
	 * �V�����R�l�N�V�������擾���܂��B<br/>
	 * ���̃R�l�N�V������{@link Connection#commit()}���R�[�����ꂽ�ꍇ�ł��R�~�b�g���ꂸ�A�g�����U�N�V�������p�����܂��B
	 * �V�����R�l�N�V�������擾�����ꍇ�́A�K��{@link #releaseConnection(Connection)}�ŉ�����Ă��������B
	 * @return	�R�l�N�V����
	 * @throws Exception
	 */
	protected Connection newConnection() throws Exception {
		return DbConnection.getConnection();
	}

	/**
	 * �R�l�N�V������������܂��B
	 * @param conn	�R�l�N�V����
	 */
	protected void releaseConnection(Connection conn) {
		DbUtils.rollbackAndCloseQuietly(conn);
	}

	/**
	 * �R�~�b�g�ƃ��[���o�b�N�𖳌��ɂ����R�l�N�V�������擾���܂��B<br/>
	 * ���̃R�l�N�V������{@link #conn}�Ɠ����g�����U�N�V�����ɂȂ�܂����A
	 * ���̃��\�b�h�Ŏ擾�����R�l�N�V�����̂݁A���[���o�b�N�������ɂȂ�܂��B<br/>
	 * �Ăяo������<code>{@link #conn}.rollback()</code>���R�[������ƁA���̃R�l�N�V�����̃g�����N�U�N�V���������[���o�b�N���邱�Ƃ��\�ł��B
	 *
	 * @return ���[���o�b�N�𖳌��ɂ����R�l�N�V����
	 */
	protected Connection getRollbackDisabledConnection() {
		return rollbackDisabledConn;
	}

	/**
	 * �t�@�C��������File�N���X��Ԃ��܂��B<br/>
	 * �t�@�C���͗��p����N���X�Ɠ��K�w�́uresources�v�p�b�P�[�W���ɑ��݂���K�v������܂��B<br/>
	 * jp.co.worksap.settle.SettleTest ���牺�L�̂悤�ɌĂяo�����ꍇ�A<br/>
	 * �t�@�C���́ujp.co.worksap.settle.resources.test.xls�v�ɂ���Ƃ݂Ȃ���܂��B
	 * <pre>
	 * getResourceFile("test.xls"); // ���\�[�X��resources�����ɂ���ꍇ
	 * getResourceFile("foo/test.xls"); // ���\�[�X��resources/foo�ɂ���ꍇ
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
	 * �t�@�C������f�[�^�x�[�X�Ƀf�[�^��}�����܂��B<br/>
	 * �t�@�C���̎w����@��{@link #loadResourceFile(String)}�̐������Q�l�ɂ��Ă��������B
	 *
	 * @param fileName
	 * @throws Exception
	 * @see #loadResourceFile(String)
	 * @see #insertFrom(String, PrimaryKey...)
	 * @return �}������{@link IDataSet}
	 */
	protected IDataSet insertFrom(String fileName) throws Exception {
		return insertFrom(fileName, (PrimaryKey[])null);
	}

	/**
	 * �t�@�C������f�[�^�x�[�X�Ƀf�[�^��}�����܂��B<br/>
	 * ���̃��\�b�h�͐ÓI�Ƀv���C�}���[�L�[���w�肷��ׁA{@link #insertFrom(String)}���������ɓ��삵�܂��B
	 * �t�@�C���̎w����@��{@link #loadResourceFile(String)}�̐������Q�l�ɂ��Ă��������B
	 *
	 * @param fileName
	 * @param primaryKeys
	 * @throws Exception
	 * @return �}������{@link IDataSet}
	 */
	protected IDataSet insertFrom(String fileName, PrimaryKey... primaryKeys) throws Exception {
		return dbOperate(DatabaseOperation.INSERT, fileName, primaryKeys);
	}

	/**
	 * �t�@�C������f�[�^�x�[�X�Ƀf�[�^��}�����܂��B<br/>
	 * ���̃��\�b�h�͐ÓI�Ƀv���C�}���[�L�[���w�肷��ׁA{@link #insertFrom(String)}���������ɓ��삵�܂��B
	 * �t�@�C���̎w����@��{@link #loadResourceFile(String)}�̐������Q�l�ɂ��Ă��������B
	 *
	 * @param fileName
	 * @param primaryKeys
	 * @throws Exception
	 * @return �}������{@link IDataSet}
	 */
	protected IDataSet refresh(String fileName, PrimaryKey... primaryKeys) throws Exception {
		return dbOperate(DatabaseOperation.REFRESH, fileName, primaryKeys);
	}

	/**
	 * DB����
	 * @param operation		DB����
	 * @param fileName		�t�@�C����
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
	 * SQL�ɂ�郌�R�[�h�擾
	 * @param tableName	�e�[�u����
	 * @param sql		SQL
	 * @return	���R�[�h�iMAP�j���X�g
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
	 * IDataSet���烌�R�[�h�擾
	 * @param dataset	�f�[�^�Z�b�g
	 * @param tableName	�e�[�u����
	 * @return	���R�[�h�iMAP�j���X�g
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
	 * ���Ғl�f�[�^�Z�b�g�擾
	 * @param testMethod	�e�X�g���\�b�h��
	 * @return	�f�[�^�Z�b�g
	 * @throws Exception
	 */
	protected IDataSet getExpectedDataset(String testMethod) throws Exception {
		// ���ʌ���
		String resourceFolder = getResourceFolder(testMethod);

		// ���̑��͌��ʃf�[�^Excel����
		String fileName = resourceFolder + "/expected.xls";

		return (ReplacementDataSet)loadResourceFile(fileName);
	}

	/**
	 * �����f�[�^��IDataSet�ɕϊ�<br>
	 * �u��������ɑΉ�����f�[�^�^���w�肷��B
	 * @param fileName	�����f�[�^�̃t�@�C����
	 * @return	����DataSet
	 * @throws Exception
	 */
	protected IDataSet loadResourceFile(String fileName) throws Exception {

		File file = getResourceFile(fileName);

		Map<Object, Object> replacements = new HashMap<Object, Object>();
		setReplacement(replacements);

		return DbUnitUtils.newDataSet(file, replacements);
	}

	/**
	 * �u��������ɑΉ�����f�[�^�^���w�肷��B
	 * @param replacements	�u�����[��Map
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
	 * �u��������ɑΉ�����f�[�^�^��ǉ��w�肷��B<br>
	 * ��̉��͌ʃe�X�g�N���X�ɂ�
	 * @param replacements	�u�����[��Map
	 */
	protected void addReplacement(Map<Object, Object> replacements) {
	}

	/**
	 * ���ҒlDataset���猟�ؑΏۃJ�������擾����
	 * @param dataset	���Ғl
	 * @param tableName	�e�[�u����
	 * @return	���ؑΏۃJ����
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
	 * ���\�[�X�t�H���_���擾����B<br>
	 * "�N���X���^���[�UID"
	 * @param testMethod
	 * @return	���\�[�X�t�H���_
	 */
	protected String getResourceFolder(String testMethod) {
		String className = this.getClass().getSimpleName();
		return className + "/" + testMethod;
	}

	/**
	 * �������<br>
	 * �����f�[�^�𓊓�����
	 * @param testMethod
	 * @param pks	primary keys
	 * @throws Throwable
	 */
	protected void prepare(String testMethod, PrimaryKey... pks) throws Throwable {

		String resourceFolder = getResourceFolder(testMethod);

		// �f�[�^����
		refresh(resourceFolder + "/input.xls");
	}

	/**
	 * �e�[�u��������
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
