package io.rodeo.chute;

// Represents an individual row
public class Row {
	private final Object[] values;

	public Row(Object[] values) {
		this.values = values;
	}

	public Object[] getValues() {
		return values;
	}

	public Key getKey(TableSchema schema) {
		int[] keyOffsets = schema.getPrimaryKeyColumnOffsets();
		Object[] keyValues = new Object[keyOffsets.length];
		for (int i = 0; i < keyOffsets.length; i++) {
			keyValues[i] = this.values[keyOffsets[i]];
		}
		return new Key(keyValues);
	}

	@Override
	public String toString() {
		return "Row: " + StringUtil.mkString(values, ":");
	}
}
