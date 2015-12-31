package io.rodeo.chute;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSetObjectArrayIterator implements Iterator<Object[]> {
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

	public ResultSetObjectArrayIterator(ResultSet resultSet) throws SQLException {
		this.resultSet = resultSet;
		iterate();
	}

	@Override
	public boolean hasNext() {
		return !done;
	}

	@Override
	public Object[] next() {
		if (done) {
			return null;
		}

		try {
			Object[] columnValues = new Object[resultSet.getMetaData().getColumnCount()];
			for (int i = 0; i < columnValues.length; i++) {
				columnValues[i] = resultSet.getObject(i + 1);
			}
			iterate();
			return columnValues;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
