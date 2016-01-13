package io.rodeo.chute.mysql;

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

import io.rodeo.chute.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSetObjectArrayIterator implements Iterator<Row> {
	private final ResultSet resultSet;
	private boolean done = false;

	private void iterate() throws SQLException {
		if (resultSet.isClosed()) {
			if (!done) {
				throw new IllegalStateException("ResultSet unexpectedly closed");
			}
			return;
		}

		done = !resultSet.next();

		if (done) {
			resultSet.close();
		}
	}

	public ResultSetObjectArrayIterator(ResultSet resultSet)
			throws SQLException {
		this.resultSet = resultSet;
		iterate();
	}

	@Override
	public boolean hasNext() {
		return !done;
	}

	@Override
	public Row next() {
		if (done) {
			return null;
		}

		try {
			Object[] columnValues = new Object[resultSet.getMetaData()
					.getColumnCount()];
			for (int i = 0; i < columnValues.length; i++) {
				columnValues[i] = resultSet.getObject(i + 1);
			}
			iterate();
			return new Row(columnValues);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
