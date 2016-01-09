package io.rodeo.chute;

/*
Copyright 2016 Fred Wulff

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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
