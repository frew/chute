package io.rodeo.chute.mysql;

import io.rodeo.chute.Key;
import io.rodeo.chute.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class MySqlFullImporter {
	private final MySqlTableSchema schema;
	private int batchSize;

	private static void setStmtFromArray(PreparedStatement stmt, int startPosition, Object[] arr)
			throws SQLException {
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
			String primaryKeyParameterList = schema.getPrimaryKeyParameterList();

			String allColumns = schema.getCommaDelimitedAllColumns();
			String initialRowsSql = "SELECT " + allColumns +
					" FROM " + schema.getTableName() +
					" WHERE (" + primaryKeyList + ") < (" + primaryKeyParameterList + ")";
			String rowsSql = "SELECT " + allColumns +
					" FROM " + schema.getTableName() +
					" WHERE (" + primaryKeyList + ") >= (" + primaryKeyParameterList + ")" +
					" AND (" + primaryKeyList + ") < (" + primaryKeyParameterList + ")";
			String finalRowsSql = "SELECT " + allColumns +
					" FROM " + schema.getTableName() +
					" WHERE (" + primaryKeyList + ") >= (" + primaryKeyParameterList + ")";
			String allRowsSql = "SELECT " + allColumns +
					" FROM " + schema.getTableName();
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
				setStmtFromArray(stmt, 1, endSplit.getValues());
			} else if (endSplit == null) {
				stmt = finalRowsStmt;
				setStmtFromArray(stmt, 1, startSplit.getValues());
			} else {
				stmt = rowsStmt;
				setStmtFromArray(stmt, 1, startSplit.getValues());
				setStmtFromArray(stmt, 1 + startSplit.getValues().length, endSplit.getValues());
			}
			ResultSet rs = stmt.executeQuery();
			return new ResultSetObjectArrayIterator(rs);
		}
	}

	private ConcurrentHashMap<Connection, PreparedStatements> preparedStatementsMap
		= new ConcurrentHashMap<Connection, PreparedStatements>();

	public MySqlFullImporter(MySqlTableSchema schema, int batchSize) {
		this.schema = schema;
		this.batchSize = batchSize;
	}

	public Iterator<Key> createSplitPointIterator(Connection conn) throws SQLException {
		return new SplitPointIterator(schema, batchSize, conn);
	}

	public Iterator<Row> getRowsForSplit(Connection conn, Key startSplit, Key endSplit)
			throws SQLException {
		PreparedStatements ps = preparedStatementsMap.get(conn);
		if (ps == null) {
			ps = new PreparedStatements(conn);
			preparedStatementsMap.put(conn, ps);
		}
		return ps.getRowsForSplit(startSplit, endSplit);
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost/chute_test"
				// + "?profileSQL=true"
						, "root", "test");
		MySqlTableSchema schema = MySqlTableSchema.readTableSchemaFromConnection(
				conn, "chute_test", "testa");
		MySqlFullImporter dumper = new MySqlFullImporter(schema, 2);
		Iterator<Key> splitIt = dumper.createSplitPointIterator(conn);
		System.out.println("Splits:");
		Key previousSplitPoint = null;
		while (splitIt.hasNext()) {
			Key splitPoint = splitIt.next();
			System.out.println("End key: " + splitPoint);
			System.out.println("In split:");
			Iterator<Row> rowIt = dumper.getRowsForSplit(conn, previousSplitPoint, splitPoint);
			while (rowIt.hasNext()) {
				Row row = rowIt.next();
				System.out.println("> " + row);
			}
			previousSplitPoint = splitPoint;
		}
		System.out.println("Final: ");
		System.out.println("In split:");
		Iterator<Row> rowIt = dumper.getRowsForSplit(conn, previousSplitPoint, null);
		while (rowIt.hasNext()) {
			Row row = rowIt.next();
			System.out.println("> " + row);
		}
	}
}
