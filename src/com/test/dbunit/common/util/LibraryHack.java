package com.test.dbunit.common.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import mockit.Delegate;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.TimestampDataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.dbunit.dataset.excel.XlsTableMock;

/**
 * テストで利用しているライブラリをモック化するユーティリティクラスです。<br/>
 * ライブラリのバージョンによって動きが変わる可能性があります。
 */
public class LibraryHack {
	/**
	 * 利用しているライブラリをモック化します。
	 */
	public static void mockup() {
		// モック対象ごとにメソッドを作る際、
		// staticメソッドだとjmockitがエラーを吐くのでインスタンスを作る
		new LibraryHack().exec();

		System.out.println("LibraryHack: mockup end.");
	}

	/** メインメソッド */
	public void exec() {
		XlsTableMock.mockup();
		mockDbunitTimestampDataType();
	}

	/**
	 * DBUnit{@link TimestampDataType}のモック化。
	 * typeCast時のパターンにスラッシュ区切りがない為。
	 */
	private void mockDbunitTimestampDataType() {
		try {
			new NonStrictExpectations() {
				@Mocked(methods = { "typeCast" })
				TimestampDataType timestampDataType;
				{
					timestampDataType.typeCast(any);
					result = new Delegate<TimestampDataType>() {
						@SuppressWarnings("unused")
						public Object typeCast(Object value) throws TypeCastException {
							//logger.debug("typeCast(value={}) - start", value);

							if (value == null || value == ITable.NO_VALUE) {
								return null;
							}

							if (value instanceof java.sql.Timestamp) {
								return value;
							}

							if (value instanceof java.util.Date) {
								java.util.Date date = (java.util.Date)value;
								return new java.sql.Timestamp(date.getTime());
							}

							if (value instanceof Long) {
								Long date = (Long)value;
								return new java.sql.Timestamp(date.longValue());
							}

							if (value instanceof String) {
								String stringValue = (String)value;

								String[] patterns = {
										// hack start: add patterns yyyy/mm/dd
										"yyyy/MM/dd HH:mm:ss.SSS Z",
										"yyyy/MM/dd HH:mm:ss.SSS",
										"yyyy/MM/dd HH:mm:ss Z",
										"yyyy/MM/dd HH:mm:ss",
										"yyyy/MM/dd HH:mm Z",
										"yyyy/MM/dd HH:mm",
										"yyyy/MM/dd Z",
										"yyyy/MM/dd",
										// hack end
										"yyyy-MM-dd HH:mm:ss.SSS Z",
										"yyyy-MM-dd HH:mm:ss.SSS",
										"yyyy-MM-dd HH:mm:ss Z",
										"yyyy-MM-dd HH:mm:ss",
										"yyyy-MM-dd HH:mm Z",
										"yyyy-MM-dd HH:mm",
										"yyyy-MM-dd Z",
										"yyyy-MM-dd", };

								for (int i = 0; i < patterns.length; ++i) {
									String p = patterns[i];
									try {
										DateFormat df = new SimpleDateFormat(p);
										Date date = df.parse(stringValue);
										return new java.sql.Timestamp(date.getTime());
									} catch (ParseException e) {
										if (i < patterns.length)
											continue;
										throw new TypeCastException(value, timestampDataType, e);
									}
								}
							}

							throw new TypeCastException(value, timestampDataType);
						}
					};
				}
			};
		} catch (TypeCastException e) {
			throw new RuntimeException(e);
		}
	}
}
