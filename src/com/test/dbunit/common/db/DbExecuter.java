package com.test.dbunit.common.db;

import java.sql.Connection;
import java.sql.Statement;

/**
 * SQL実行
 * @author la_song
 *
 */
public class DbExecuter {

	/**
	 * インスタンス化抑制用コンストラクタ
	 */
	private DbExecuter() {
	}

	/**
	 * 更新系のSQLを実行する
	 * @param sqlKey	SQLキー
	 * @param params	置換文字列
	 * @throws Exception
	 */
	public static void update(String sqlKey, Object... params) throws Exception {
		Connection conn = null;
		Statement stmt = null;
		try {
			String sql = SQLs.getSql(sqlKey, params);
			System.out.println(sql);

			conn = DbConnection.getConnection();
			conn.setAutoCommit(true);
			stmt = conn.createStatement();

			stmt.executeUpdate(sql);

		} finally {
			if (stmt != null) {
				stmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

	}
}
