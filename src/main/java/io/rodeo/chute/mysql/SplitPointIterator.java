package io.rodeo.chute.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class SplitPointIterator implements Iterator<Object[]> {
	private int batchSize;

	private PreparedStatement initialSplitStmt;
	private PreparedStatement splitStmt;

	private boolean done = false;

	private Object[] nextSplitPoint = null;

	public SplitPointIterator(MySqlTableSchema schema, int batchSize, Connection connection) throws SQLException {
		this.batchSize = batchSize;

		String primaryKeyList = schema.getCommaDelimitedPrimaryKeyColumns();
		String primaryKeyParameterList = schema.getPrimaryKeyParameterList();

		String initialSplitSql = "SELECT " + primaryKeyList + " FROM " + schema.getTableName() +
				" LIMIT 1 OFFSET ?";
		String splitSql = "SELECT " + primaryKeyList + " FROM " + schema.getTableName() +
				" WHERE (" + primaryKeyList + ") >= (" + primaryKeyParameterList +
				") LIMIT 1 OFFSET ?";
		initialSplitStmt = connection.prepareStatement(initialSplitSql);
		splitStmt = connection.prepareStatement(splitSql);

		initialSplitStmt.setInt(1, batchSize);
		ResultSet rs = initialSplitStmt.executeQuery();
		if (!rs.next()) {
			done = true;
			return;
		}
		nextSplitPoint = new Object[rs.getMetaData().getColumnCount()];
		for (int i = 0; i < nextSplitPoint.length; i++) {
			nextSplitPoint[i] = rs.getObject(i + 1);
		}
	}

	@Override
	public boolean hasNext() {
		return !done;
	}

	@Override
	public Object[] next() {
		Object[] currentSplitPoint = nextSplitPoint;

		try {
			for (int i = 0; i < currentSplitPoint.length; i++) {
				splitStmt.setObject(i + 1, currentSplitPoint[i]);
			}
			splitStmt.setInt(currentSplitPoint.length + 1, batchSize);

			ResultSet rs = splitStmt.executeQuery();

			if (!rs.next()) {
				done = true;
				nextSplitPoint = null;
				return currentSplitPoint;
			}

			nextSplitPoint = new Object[currentSplitPoint.length];

			for (int i = 0; i < currentSplitPoint.length; i++) {
				nextSplitPoint[i] = rs.getObject(i + 1);
			}

			return currentSplitPoint;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
