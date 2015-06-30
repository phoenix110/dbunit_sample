package com.test.dbunit.common.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.filter.IColumnFilter;
import org.dbunit.operation.DatabaseOperation;

/**
 * {@link IColumnFilter}を静的に定義するクラスです。<br/>
 * このクラスを利用することで、DBUnitの{@link DatabaseOperation}を高速化します。
 */
public class StaticColumnFilter implements IColumnFilter {

	private Map<String, List<String>> tablePrimaryKeyMap = new HashMap<String, List<String>>();

	/**
	 * @param primaryKeys 指定する{@link PrimaryKey}
	 */
	public StaticColumnFilter(PrimaryKey... primaryKeys) {
		if (primaryKeys == null || primaryKeys.length == 0) {
			return;
		}
		for (PrimaryKey pk : primaryKeys) {
			tablePrimaryKeyMap.put(pk.getTableName(), Arrays.asList(pk.getColumnNames()));
		}
	}

	@Override
	public boolean accept(String tableName, Column column) {
		if (tablePrimaryKeyMap.containsKey(tableName)) {
			return tablePrimaryKeyMap.get(tableName).contains(column.getColumnName());
		} else {
			return false;
		}
	}

	/**
	 * プライマリキーを定義します。
	 */
	public static class PrimaryKey {
		private String tableName;
		private String[] columnNames;

		/**
		 * @param tableName テーブル名
		 * @param columnNames プライマリキー
		 */
		public PrimaryKey(String tableName, String[] columnNames) {
			this.tableName = tableName;
			this.columnNames = columnNames;
		}

		/**
		 * @return テーブル名
		 */
		public String getTableName() {
			return this.tableName;
		}

		/**
		 * @return プライマリキー
		 */
		public String[] getColumnNames() {
			return this.columnNames;
		}

		public static PrimaryKey[] join(Object... keys) {
			return joinObjects(keys).toArray(new PrimaryKey[0]);
		}

		public static String[] joinColumns(Object... columns) {
			return joinObjects(columns).toArray(new String[0]);
		}

		@SuppressWarnings("unchecked")
		private static <T> List<T> joinObjects(Object... objects) {
			List<T> list = new ArrayList<T>();
			for (Object obj : objects) {
				if (obj.getClass().isArray()) {
					int length = Array.getLength(obj);
					for (int idx = 0; idx < length; idx++) {
						list.add((T)Array.get(obj, idx));
					}
				} else {
					list.add((T)obj);
				}
			}
			return list;
		}
	}
}