package io.rodeo.chute;

public class MySqlColumnSchema {
	private final String columnName;
	private final ChuteType columnType;
	private final int columnLen;
	
	public MySqlColumnSchema(String name, ChuteType type, int len) {
		columnName = name;
		columnType = type;
		columnLen = len;
	}

	public String getColumnName() {
		return columnName;
	}

	public ChuteType getColumnType() {
		return columnType;
	}

	public int getColumnLen() {
		return columnLen;
	}
	
	@Override public String toString() {
		return columnName + " - " + columnType + "(" + columnLen +")";
	}
}