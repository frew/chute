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

import io.rodeo.chute.Key;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class SplitPointIterator implements Iterator<Key> {
	private int batchSize;

	private PreparedStatement initialSplitStmt;
	private PreparedStatement splitStmt;

	private boolean done = false;

	private Object[] nextSplitPoint = null;

	public SplitPointIterator(MySqlTableSchema schema, int batchSize,
			Connection connection) throws SQLException {
		this.batchSize = batchSize;

		String primaryKeyList = schema.getCommaDelimitedPrimaryKeyColumns();
		String primaryKeyParameterList = schema.getPrimaryKeyParameterList();

		String initialSplitSql = "SELECT " + primaryKeyList + " FROM "
				+ schema.getTableName() + " LIMIT 1 OFFSET ?";
		String splitSql = "SELECT " + primaryKeyList + " FROM "
				+ schema.getTableName() + " WHERE (" + primaryKeyList
				+ ") >= (" + primaryKeyParameterList + ") LIMIT 1 OFFSET ?";
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
	public Key next() {
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
				return new Key(currentSplitPoint);
			}

			nextSplitPoint = new Object[currentSplitPoint.length];

			for (int i = 0; i < currentSplitPoint.length; i++) {
				nextSplitPoint[i] = rs.getObject(i + 1);
			}

			return new Key(currentSplitPoint);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
