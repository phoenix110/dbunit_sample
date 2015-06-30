package com.test.dbunit.common.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlProducer;
import org.xml.sax.InputSource;

/**
 * <code>DbUnitUtils</code>
 * <pre>
 * Utilities for using <code>org.dbunit</code> on <code>jp.co.worksap.companyac.testcommon</code> .
 * </pre>
 * @author iwado
 */
public enum DbUnitUtils {
	;

	private static Log log = LogFactory.getLog(DbUnitUtils.class);

	/**
	 * Create new {@link IDataSet} from specified file and replacement {@link Map}.<br>
	 * If the target file includes key of replacement {@link Map}, those data will be replaced by the corresponding value.
	 * @param file
	 * @param replacements
	 * @return {@link IDataSet}
	 */
	public static IDataSet newDataSet(File file, Map<Object, Object> replacements) {
		ReplacementDataSet result = (ReplacementDataSet)newDataSet(file);
		for (Object key : replacements.keySet()) {
			result.addReplacementObject(key, replacements.get(key));
		}
		return result;
	}

	/**
	 * Create new {@link IDataSet} from specified file.
	 * @param file
	 * @return {@link IDataSet}
	 */
	public static IDataSet newDataSet(File file) {
		ReplacementDataSet result;
		try {
			result = new ReplacementDataSet(DbUnitUtils.newDataSetFromFile(file));
			for (Replacement replacement : Replacement.allOf()) {
				result.addReplacementObject(replacement.key(), replacement.value());
			}
			return result;
		} catch (FileNotFoundException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Export {@link IDataSet} to Microsoft EXCEL file.
	 * @param dataSet
	 * @param file
	 */
	public static void exportDataSetToXls(IDataSet dataSet, File file) {
		FileOutputStream output = null;
		HSSFWorkbook book = null;
		try {
			if (!FileNameUtils.isExtensionIgnoreCase(file, "xls"))
				FileNameUtils.changeExtension(file, "xls");
			if (!file.exists()) {
				createNewFileAndDirectory(file);
			}
			output = new FileOutputStream(file);
			book = new HSSFWorkbook();
			for (String tableName : dataSet.getTableNames()) {
				ITable table = dataSet.getTable(tableName);
				HSSFSheet sheet = book.createSheet(tableName);
				exportTableToXlsSheet(table, book, sheet);
			}
			book.write(output);
		} catch (DataSetException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} catch (FileNotFoundException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} catch (IOException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} finally {
			IOUtils.closeQuietly(output);
		}
	}

	/**
	 * Export {@link IDataSet} to CSV files.
	 * @param dataSet
	 * @param dir
	 */
	public static void exportDataSetToCsv(IDataSet dataSet, File dir) {
		FileOutputStream output = null;
		try {
			if (!FileNameUtils.isExtensionIgnoreCase(dir, ""))
				FileNameUtils.changeExtension(dir, "");
			if (!dir.exists() || !dir.isDirectory()) {
				dir.mkdirs();
			}
			for (String tableName : dataSet.getTableNames()) {
				ITable table = dataSet.getTable(tableName);
				File file = new File(dir, tableName + ".csv");
				if (file.exists())
					file.delete();
				file.createNewFile();
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				try {
					exportTableToCsv(table, writer);
					writer.flush();
				} finally {
					IOUtils.closeQuietly(writer);
				}
			}
		} catch (DataSetException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} catch (FileNotFoundException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} catch (IOException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} finally {
			IOUtils.closeQuietly(output);
		}
	}

	/**
	 * @param conn
	 * @return {@link DatabaseConnection}
	 */
	public static IDatabaseConnection getDbUnitConnection(Connection conn, String schema) {
		try {
			return new DatabaseConnection(conn, schema);
		} catch (DatabaseUnitException e) {
			log.fatal(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Close {@link DatabaseConnection} quietly.<br>
	 * Close internal {@link Connection} as well.
	 * @param dbConn
	 */
	public static void closeQuietly(IDatabaseConnection dbConn) {
		try {
			DbUtils.closeQuietly(dbConn.getConnection());
			dbConn.close();
		} catch (SQLException ignore) {
		}
	}

	/**
	 * Close {@link DatabaseConnection} quietly.<br>
	 * Rollback and close internal {@link Connection}.
	 * @param dbConn
	 */
	public static void rollbackAndCloseQuietly(IDatabaseConnection dbConn) {
		try {
			DbUtils.rollbackAndCloseQuietly(dbConn.getConnection());
			dbConn.close();
		} catch (SQLException ignore) {
		}
	}

	/**
	 * Close {@link DatabaseConnection} quietly.<br>
	 * Commit and close internal {@link Connection}.
	 * @param dbConn
	 */
	public static void commitAndCloseQuietly(IDatabaseConnection dbConn) {
		try {
			dbConn.getConnection().commit();
			closeQuietly(dbConn);
		} catch (SQLException ignore) {
		}
	}

	private static void createNewFileAndDirectory(File file) throws IOException {
		if (!file.getParentFile().exists() || !file.getParentFile().isDirectory())
			file.getParentFile().mkdirs();
		file.createNewFile();
	}

	private static void exportTableToXlsSheet(ITable table, HSSFWorkbook book, HSSFSheet sheet) {
		try {
			ExcelBook excelBook = new ExcelBook(book);
			// Create Header
			HSSFRow header = sheet.createRow(0);
			int c = 0;
			for (Column column : table.getTableMetaData().getColumns()) {
				HSSFCell cell = header.createCell(c);
				c++;
				cell.setCellStyle(excelBook.getCellStyle(ExcelBook.ECellStyleFormat.TEXT));
				cell.setCellValue(new HSSFRichTextString(column.getColumnName()));
			}
			// Create Data Rows
			for (int i = 0; i < table.getRowCount(); i++) {
				HSSFRow row = sheet.createRow(i + 1);//add for Header
				c = 0;
				for (Column column : table.getTableMetaData().getColumns()) {
					HSSFCell cell = row.createCell(c);
					c++;
					CellDataType.of(column.getDataType()).setCellValue(excelBook, cell,
							table.getValue(i, column.getColumnName()));
				}
			}
		} catch (DataSetException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		}
	}

	private static void exportTableToCsv(ITable table, BufferedWriter writer) {
		try {
			List<String> record = new ArrayList<String>();
			// Create Header
			for (Column column : table.getTableMetaData().getColumns()) {
				record.add(column.getColumnName());
			}
			writer.write(StringUtils.join(record.iterator(), ","));
			writer.newLine();
			record.clear();
			// Create Data Rows
			for (int i = 0; i < table.getRowCount(); i++) {
				record.clear();
				for (Column column : table.getTableMetaData().getColumns()) {
					record.add(CellDataType.of(column.getDataType()).getStringValue(
							table.getValue(i, column.getColumnName())));
				}
				writer.write(StringUtils.join(record.iterator(), ","));
				writer.newLine();
			}
		} catch (DataSetException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} catch (IOException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		}
	}

	private static IDataSet newDataSetFromFile(File file) throws FileNotFoundException {
		if (!file.exists()) {
			throw new FileNotFoundException("Data file was not found: file=" + file);
		}
		try {
			if (file.isFile()) {
				if (FilenameUtils.getExtension(file.getName()).equalsIgnoreCase("XLS"))
					return new XlsDataSet(file);
				if (FilenameUtils.getExtension(file.getName()).equalsIgnoreCase("XML"))
					return new FlatXmlDataSet(new FlatXmlProducer(new InputSource(new FileInputStream(file))));
			} else if (file.isDirectory()) {
				return new CsvDataSet(file);
			}
			throw new IllegalStateException(
					"Correct data file was not found. File extension must be XLS, XML or Directory which includes only CSV: file="
							+ file);
		} catch (DataSetException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		} catch (IOException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new IllegalStateException(e);
		}
	}

	private enum Replacement {
		NULL("$NULL") {
			@Override
			Object value() {
				return null;
			}
		},
		BLANK("$BLANK") {
			@Override
			Object value() {
				return "";
			}
		},
		RANDOM_BYTE("$RANDOM_BYTE") {
			@Override
			Object value() {
				return Byte.valueOf(Integer.valueOf(RandomUtils.nextInt()).byteValue());
			}
		},
		RANDOM_SHORT("$RANDOM_SHORT") {
			@Override
			Object value() {
				return Short.valueOf(Integer.valueOf(RandomUtils.nextInt()).shortValue());
			}
		},
		RANDOM_INT("$RANDOM_INT") {
			@Override
			Object value() {
				return Integer.valueOf(RandomUtils.nextInt() * getRandomSign());
			}
		},
		RANDOM_LONG("$RANDOM_LONG") {
			@Override
			Object value() {
				return Long.valueOf(RandomUtils.nextLong() * getRandomSign());
			}
		},
		RAMDOM_DOUBLE("$RAMDOM_DOUBLE") {
			@Override
			Object value() {
				return Double.valueOf(RandomUtils.nextDouble() * getRandomSign());
			}
		},
		RAMDOM_BOOL("$RAMDOM_BOOL") {
			@Override
			Object value() {
				return Boolean.valueOf(RandomUtils.nextBoolean());
			}
		},
		RAMDOM_ASCII_10("$RAMDOM_ASCII_10") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(10);
			}
		},
		RAMDOM_ASCII_50("$RAMDOM_ASCII_50") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(50);
			}
		},
		RAMDOM_ASCII_100("$RAMDOM_ASCII_100") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(100);
			}
		},
		RAMDOM_ALPHA_10("$RAMDOM_ALPHA_10") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(10);
			}
		},
		RAMDOM_ALPHA_50("$RAMDOM_ALPHA_50") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(50);
			}
		},
		RAMDOM_ALPHA_100("$RAMDOM_ALPHA_100") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(100);
			}
		},
		RAMDOM_NUMERIC_10("$RAMDOM_NUMERIC_10") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(10);
			}
		},
		RAMDOM_NUMERIC_50("$RAMDOM_NUMERIC_50") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(50);
			}
		},
		RAMDOM_NUMERIC_100("$RAMDOM_NUMERIC_100") {
			@Override
			Object value() {
				return RandomStringUtils.randomAscii(100);
			}
		},
		CURRENT_DATE("$CURRENT_DATE") {
			@Override
			Object value() {
				return new Date(System.currentTimeMillis());
			}
		},
		MIN_DATE("$MIN_DATE") {
			@Override
			Object value() {
				return MINIMUM_DATE;
			}
		},
		MAX_DATE("$MAX_DATE") {
			@Override
			Object value() {
				return MAXIMUM_DATE;
			}
		},
		CURRENT_TIMESTAMP("$CURRENT_TIMESTAMP") {
			@Override
			Object value() {
				return new Timestamp(System.currentTimeMillis());
			}
		},
		MIN_TIMESTAMP("$MIN_TIMESTAMP") {
			@Override
			Object value() {
				return MINIMUM_TIMESTAMP;
			}
		},
		MAX_TIMESTAMP("$MAX_TIMESTAMP") {
			@Override
			Object value() {
				return MAXIMUM_TIMESTAMP;
			}
		};

		private Replacement(String key) {
			this.key = key;
		}

		private static final Collection<Replacement> ALL_OF = Arrays.asList(values());
		private static Date MINIMUM_DATE;
		private static Date MAXIMUM_DATE;
		private static Timestamp MINIMUM_TIMESTAMP;
		private static Timestamp MAXIMUM_TIMESTAMP;
		static {
			Calendar cal = Calendar.getInstance();
			cal.set(1585, 1, 1, 0, 0, 0);
			cal.set(Calendar.MILLISECOND, 0);
			MINIMUM_DATE = new Date(cal.getTimeInMillis());
			MINIMUM_TIMESTAMP = new Timestamp(cal.getTimeInMillis());
			cal.set(2382, 12, 31, 23, 59, 59);
			cal.set(Calendar.MILLISECOND, 999);
			MAXIMUM_DATE = new Date(cal.getTimeInMillis());
			MAXIMUM_TIMESTAMP = new Timestamp(cal.getTimeInMillis());
		}

		private final String key;

		static Collection<Replacement> allOf() {
			return ALL_OF;
		}

		String key() {
			return key;
		}

		abstract Object value();

		private static int getRandomSign() {
			if (RandomUtils.nextBoolean()) {
				return 1;
			} else {
				return -1;
			}
		}
	}

	private enum CellDataType {
		BOOLEAN(DataType.BOOLEAN) {
			@Override
			void setCellValue(ExcelBook excelBook, HSSFCell cell, Object value) {
				cell.setCellValue(((Boolean)value).booleanValue());
			}

			@Override
			String getStringValue(Object value) {
				return String.valueOf((Boolean)value);
			}
		},
		DATE(DataType.DATE, DataType.TIME, DataType.TIMESTAMP) {
			@Override
			void setCellValue(ExcelBook excelBook, HSSFCell cell, Object value) {
				if (value != null) {
					cell.setCellType(HSSFCell.CELL_TYPE_NUMERIC);
					if (value instanceof Timestamp) {
						Timestamp t = (Timestamp)value;
						Calendar c = Calendar.getInstance();
						c.setTimeInMillis(t.getTime());
						if (c.get(Calendar.HOUR_OF_DAY) == 0
								&& c.get(Calendar.MINUTE) == 0
								&& c.get(Calendar.SECOND) == 0
								&& c.get(Calendar.MILLISECOND) == 0) {
							cell.setCellStyle(excelBook.getCellStyle(ExcelBook.ECellStyleFormat.DATE3));
						} else {
							cell.setCellStyle(excelBook.getCellStyle(ExcelBook.ECellStyleFormat.DATETIME));
						}
					} else {
						cell.setCellStyle(excelBook.getCellStyle(ExcelBook.ECellStyleFormat.DATE3));
					}

					cell.setCellValue((Timestamp)value);
				}
			}

			@Override
			String getStringValue(Object value) {
				if (value == null) {
					return "";
				}
				return DATE_FORMAT.format((Date)value);
			}
		},
		NUMERIC(DataType.BIGINT, DataType.BIGINT_AUX_LONG, DataType.BIT, DataType.DECIMAL, DataType.DOUBLE,
				DataType.FLOAT, DataType.INTEGER, DataType.NUMERIC, DataType.REAL, DataType.SMALLINT, DataType.TINYINT) {
			@Override
			void setCellValue(ExcelBook excelBook, HSSFCell cell, Object value) {
				double d;
				if (value instanceof BigDecimal) {
					d = ((BigDecimal)value).doubleValue();
				} else {
					d = ((Double)value).doubleValue();
				}
				cell.setCellType(HSSFCell.CELL_TYPE_NUMERIC);
				cell.setCellStyle(excelBook.getCellStyle(ExcelBook.ECellStyleFormat.GENERAL));
				cell.setCellValue(d);
			}

			@Override
			String getStringValue(Object value) {
				return String.valueOf((Number)value);
			}
		},
		STRING(DataType.CHAR, DataType.LONGNVARCHAR, DataType.LONGVARCHAR, DataType.NCHAR, DataType.VARCHAR) {
			@Override
			void setCellValue(ExcelBook excelBook, HSSFCell cell, Object value) {
				cell.setCellType(HSSFCell.CELL_TYPE_STRING);
				cell.setCellStyle(excelBook.getCellStyle(ExcelBook.ECellStyleFormat.TEXT));
				cell.setCellValue(new HSSFRichTextString((String)value));

				System.out.println(String.valueOf(value));
			}

			@Override
			String getStringValue(Object value) {
				return (String)value;
			}
		};

		private CellDataType(DataType... types) {
			this.types = types;
		}

		private static final DateFormat DATE_FORMAT = DateFormat.getDateInstance();

		private static final Collection<CellDataType> ALL_OF = Arrays.asList(values());

		private final DataType[] types;

		static CellDataType of(DataType type) {
			for (CellDataType each : allOf()) {
				if (Arrays.asList(each.types).contains(type))
					return each;
			}
			throw new IllegalArgumentException("Unsupported DataType: type=" + type);
		}

		static Collection<CellDataType> allOf() {
			return ALL_OF;
		}

		abstract void setCellValue(ExcelBook excelBook, HSSFCell cell, Object value);

		abstract String getStringValue(Object value);
	}

	private static class ExcelBook {
		public enum ECellStyleFormat {
			/** General */
			GENERAL("General"),
			/** 0 */
			NUM1("0"),
			/** 0.00 */
			NUM2("0.00"),
			/** #,##0 */
			NUM3("#,##0"),
			/** #,##0.00 */
			NUM4("#,##0.00"),
			/** ($#,##0_);($#,##0) */
			NUM5("($#,##0_);($#,##0)"),
			/** ($#,##0_);[Red]($#,##0) */
			NUM6("($#,##0_);[Red]($#,##0)"),
			/** ($#,##0.00);($#,##0.00) */
			NUM7("($#,##0.00);($#,##0.00)"),
			/** ($#,##0.00_);[Red]($#,##0.00) */
			NUM8("($#,##0.00_);[Red]($#,##0.00)"),
			/** 0% */
			PERCENT1("0%"),
			/** 0.00% */
			PERCENT2("0.00%"),
			/** 0.00E+00 */
			EXPONENT1("0.00E+00"),
			/** # ?/? */
			DATE1("# ?/?"),
			/** # ??/?? */
			DATE2("# ??/??"),
			/** m/d/yy */
			DATE3("m/d/yy"),
			/** d-mmm-yy */
			DATE4("d-mmm-yy"),
			/** d-mmm */
			DATE5("d-mmm"),
			/** mmm-yy */
			DATE6("mmm-yy"),
			/** h:mm AM/PM */
			TIME1("h:mm AM/PM"),
			/** h:mm:ss AM/PM */
			TIME2("h:mm:ss AM/PM"),
			/** h:mm */
			TIME3("h:mm"),
			/** h:mm:ss */
			TIME4("h:mm:ss"),
			/** m/d/yy h:mm */
			DATETIME("m/d/yy h:mm"),
			/** (#,##0_);[Red](#,##0) */
			NUM9("(#,##0_);[Red](#,##0)"),
			/** (#,##0.00_);(#,##0.00) */
			NUM10("(#,##0.00_);(#,##0.00)"),
			/** (#,##0.00_);[Red](#,##0.00) */
			NUM11("(#,##0.00_);[Red](#,##0.00)"),
			/** _(*#,##0_);_(*(#,##0);_(* \"-\"_);_(@_) */
			NUM12("_(*#,##0_);_(*(#,##0);_(* \"-\"_);_(@_)"),
			/** _($*#,##0_);_($*(#,##0);_($* \"-\"_);_(@_) */
			NUM13("_($*#,##0_);_($*(#,##0);_($* \"-\"_);_(@_)"),
			/** _(*#,##0.00_);_(*(#,##0.00);_(*\"-\"??_);_(@_) */
			NUM14("_(*#,##0.00_);_(*(#,##0.00);_(*\"-\"??_);_(@_)"),
			/** _($*#,##0.00_);_($*(#,##0.00);_($*\"-\"??_);_(@_) */
			NUM15("_($*#,##0.00_);_($*(#,##0.00);_($*\"-\"??_);_(@_)"),
			/** mm:ss */
			TIME5("mm:ss"),
			/** [h]:mm:ss */
			TIME6("[h]:mm:ss"),
			/** mm:ss.0 */
			TIME7("mm:ss.0"),
			/** ##0.0E+0 */
			EXPONENT2("##0.0E+0"),
			/** @ */
			AT("@"),
			/** text */
			TEXT("text");

			private ECellStyleFormat(String format) {
				this.format = format;
			}

			public String getFormat() {
				return format;
			}

			private String format;
		}

		private HSSFWorkbook book;
		private HSSFCellStyle[] styles;
		private ECellStyleFormat[] formats;

		public ExcelBook(HSSFWorkbook book) {
			this.book = book;

			formats = ECellStyleFormat.values();
			styles = new HSSFCellStyle[formats.length];
			for (int idx = 0; idx < formats.length; idx++) {
				styles[idx] = book.createCellStyle();
				styles[idx].setDataFormat(HSSFDataFormat.getBuiltinFormat(formats[idx].getFormat()));
			}
		}

		public HSSFCellStyle getCellStyle(ECellStyleFormat format) {
			return styles[ArrayUtils.indexOf(formats, format)];

		}

	}
}
