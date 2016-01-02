package io.rodeo.chute;

public interface TableSchema {
	public ColumnSchema[] getColumns();
	public int[] getPrimaryKeyColumnOffsets();
}
