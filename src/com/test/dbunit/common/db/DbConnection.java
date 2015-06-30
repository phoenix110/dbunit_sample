package com.test.dbunit.common.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ResourceBundle;

/**
 * DBコネクション取得
 * @author c.k.song
 *
 */
public class DbConnection {

	/** db.properties */
	private static ResourceBundle bundle = ResourceBundle.getBundle("db");

	/**
	 * インスタンス化抑制用コンストラクタ
	 */
	private DbConnection() {
	}

	/**
	 * DBコネクションを取得する。
	 * アクセス情報はdb.propertiesから取得する
	 * @return	DBコネクション
	 * @throws Exception
	 */
	public static Connection getConnection() throws Exception {
		String driver = bundle.getString("db.driver");
		String url = bundle.getString("db.url");
		String user = bundle.getString("db.user");
		String password = bundle.getString("db.password");

		return getConnection(driver, url, user, password);
	}

	/**
	 * DBスキーマを取得する。
	 * アクセス情報はdb.propertiesから取得する
	 * @return	 DBスキーマ
	 * @throws Exception
	 */
	public static String getSchema() throws Exception {
		return bundle.getString("db.schema");
	}

	/**
	 * DBコネクションを取得する。
	 * @param driver	JDBCドライバー名
	 * @param url		DBのURL
	 * @param user		DBユーザID
	 * @param password	DBユーザのパスワード
	 * @return	DBコネクション
	 * @throws Exception
	 */
	public static Connection getConnection(String driver, String url, String user, String password) throws Exception {
		//JDBCドライバーをセット
		Class.forName(driver);
		//Connectionの取得
		Connection conn = DriverManager.getConnection(url, user, password);

		return conn;

	}
}
