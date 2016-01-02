package io.rodeo.chute;

public class Key {
	private final Object[] values;

	public Key(Object[] values) {
		this.values = values;
	}

	public Object[] getValues() {
		return values;
	}

	@Override
	public String toString() {
		return "Key: " + StringUtil.mkString(values, ":");
	}
}
