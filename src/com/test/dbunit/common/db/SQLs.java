package com.test.dbunit.common.db;

import java.util.ResourceBundle;

/**
 * SQL文取得
 * @author 宋
 *
 */
public class SQLs {

	/** sql.properties */
	private static ResourceBundle bundle = ResourceBundle.getBundle("sql");

	/**
	 * SQL文取得
	 * @param key	キー
	 * @return	SQL文
	 */
	public static String getSql(String key) {
		String sql = bundle.getString(key);
		return sql;
	}

	/**
	 * SQL文取得
	 * @param key	キー
	 * @param params	置換文字列
	 * @return	SQL文
	 */
	public static String getSql(String key, Object... params) {
		String pattern = bundle.getString(key);
		String sql = String.format(pattern, params);
		return sql;
	}
}
