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
import io.rodeo.chute.Row;
import io.rodeo.chute.Split;
import io.rodeo.chute.StreamPosition;
import io.rodeo.chute.StreamProcessor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MySqlFullSplitImporter {
	private static void fillPreparedStatementFromArray(PreparedStatement stmt,
			int startPosition, Object[] arr) throws SQLException {
		for (int i = 0; i < arr.length; i++) {
			stmt.setObject(startPosition + i, arr[i]);
		}
	}

	private class PreparedStatements {
		private PreparedStatement initialRowsStmt;
		private PreparedStatement rowsStmt;
		private PreparedStatement finalRowsStmt;
		private PreparedStatement allRowsStmt;

		public PreparedStatements(Connection connection) throws SQLException {
			String primaryKeyList = schema.getCommaDelimitedPrimaryKeyColumns();
			String primaryKeyParameterList = schema
					.getPrimaryKeyParameterList();

			String allColumns = schema.getCommaDelimitedAllColumns();
			String initialRowsSql = "SELECT " + allColumns + " FROM "
					+ schema.getTableName() + " WHERE (" + primaryKeyList
					+ ") < (" + primaryKeyParameterList + ")";
			String rowsSql = "SELECT " + allColumns + " FROM "
					+ schema.getTableName() + " WHERE (" + primaryKeyList
					+ ") >= (" + primaryKeyParameterList + ")" + " AND ("
					+ primaryKeyList + ") < (" + primaryKeyParameterList + ")";
			String finalRowsSql = "SELECT " + allColumns + " FROM "
					+ schema.getTableName() + " WHERE (" + primaryKeyList
					+ ") >= (" + primaryKeyParameterList + ")";
			String allRowsSql = "SELECT " + allColumns + " FROM "
					+ schema.getTableName();
			initialRowsStmt = connection.prepareStatement(initialRowsSql);
			rowsStmt = connection.prepareStatement(rowsSql);
			finalRowsStmt = connection.prepareStatement(finalRowsSql);
			allRowsStmt = connection.prepareStatement(allRowsSql);
		}

		public Iterator<Row> getRowsForSplit(Key startSplit, Key endSplit)
				throws SQLException {
			PreparedStatement stmt = null;
			if (startSplit == null && endSplit == null) {
				stmt = allRowsStmt;
			} else if (startSplit == null) {
				stmt = initialRowsStmt;
				fillPreparedStatementFromArray(stmt, 1, endSplit.getValues());
			} else if (endSplit == null) {
				stmt = finalRowsStmt;
				fillPreparedStatementFromArray(stmt, 1, startSplit.getValues());
			} else {
				stmt = rowsStmt;
				fillPreparedStatementFromArray(stmt, 1, startSplit.getValues());
				fillPreparedStatementFromArray(stmt,
						1 + startSplit.getValues().length, endSplit.getValues());
			}
			ResultSet rs = stmt.executeQuery();
			return new ResultSetObjectArrayIterator(rs);
		}
	}

	private static final ConcurrentHashMap<Connection, PreparedStatements> preparedStatementsMap = new ConcurrentHashMap<Connection, PreparedStatements>();

	private final MySqlTableSchema schema;
	private int batchSize;

	public MySqlFullSplitImporter(MySqlTableSchema schema, int batchSize) {
		this.schema = schema;
		this.batchSize = batchSize;
	}

	public Iterator<Key> createSplitPointIterator(Connection conn)
			throws SQLException {
		return new SplitPointIterator(schema, batchSize, conn);
	}

	public class FullImporterRunnable implements Runnable {
		private final Connection conn;
		private final int epoch;
		private final Split split;
		private final List<StreamProcessor> processors;
		private final Runnable cb;

		public FullImporterRunnable(Connection conn, int epoch, Split split,
				List<StreamProcessor> processors, Runnable cb) {
			this.conn = conn;
			this.epoch = epoch;
			this.split = split;
			this.processors = processors;
			this.cb = cb;
		}

		public Iterator<Row> getRowsForSplit() throws SQLException {
			PreparedStatements ps = preparedStatementsMap.get(conn);
			if (ps == null) {
				ps = new PreparedStatements(conn);
				preparedStatementsMap.put(conn, ps);
			}
			return ps.getRowsForSplit(split.getStartKey(), split.getEndKey());
		}

		@Override
		public void run() {
			StreamPosition pos = new MySqlStreamPosition(this.epoch, "", 0);
			Iterator<Row> rowIt;
			try {
				rowIt = getRowsForSplit();

				while (rowIt.hasNext()) {
					Row row = rowIt.next();
					for (StreamProcessor processor : processors) {
						processor.process(schema, null, row, pos);
					}
				}
			} catch (SQLException e) {
				// TODO: Less disruptive handling
				e.printStackTrace();
				System.exit(1);
			}
			cb.run();
		}
	}

	public FullImporterRunnable createImporterRunnable(Connection conn,
			int epoch, Split split, List<StreamProcessor> processors,
			Runnable cb) {
		return new FullImporterRunnable(conn, epoch, split, processors, cb);
	}
}
