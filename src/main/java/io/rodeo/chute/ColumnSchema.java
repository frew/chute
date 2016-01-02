package io.rodeo.chute;

public class ColumnSchema {
	private final String columnName;
	private final ColumnType columnType;
	private final int columnLen;

	public ColumnSchema(String name, ColumnType type, int len) {
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

	@Override
	public String toString() {
		return columnName + " - " + columnType + "(" + columnLen + ")";
	}
}
