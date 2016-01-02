package io.rodeo.chute.mysql;

import io.rodeo.chute.ColumnType;

public class MySqlColumnSchema {
	private final String columnName;
	private final ColumnType columnType;
	private final int columnLen;
	
	public MySqlColumnSchema(String name, ColumnType type, int len) {
		columnName = name;
		columnType = type;
		columnLen = len;
	}

	public String getColumnName() {
		return columnName;
	}

	public ColumnType getColumnType() {
		return columnType;
	}

	public int getColumnLen() {
		return columnLen;
	}
	
	@Override public String toString() {
		return columnName + " - " + columnType + "(" + columnLen +")";
	}
}