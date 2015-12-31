package io.rodeo.chute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class MySqlFullDumper {
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

		public Iterator<Object[]> getRowsForSplit(Object[] startSplit, Object[] endSplit)
				throws SQLException {
			PreparedStatement stmt = null;
			if (startSplit == null && endSplit == null) {
				stmt = allRowsStmt;
			} else if (startSplit == null) {
				stmt = initialRowsStmt;
				setStmtFromArray(stmt, 1, endSplit);
			} else if (endSplit == null) {
				stmt = finalRowsStmt;
				setStmtFromArray(stmt, 1, startSplit);
			} else {
				stmt = rowsStmt;
				setStmtFromArray(stmt, 1, startSplit);
				setStmtFromArray(stmt, 1 + startSplit.length, endSplit);
			}
			ResultSet rs = stmt.executeQuery();
			return new ResultSetObjectArrayIterator(rs);
		}
	}

	private ConcurrentHashMap<Connection, PreparedStatements> preparedStatementsMap
		= new ConcurrentHashMap<Connection, PreparedStatements>();

	public MySqlFullDumper(MySqlTableSchema schema, int batchSize) {
		this.schema = schema;
		this.batchSize = batchSize;
	}

	public Iterator<Object[]> createSplitPointIterator(Connection conn) throws SQLException {
		return new SplitPointIterator(schema, batchSize, conn);
	}

	public Iterator<Object[]> getRowsForSplit(Connection conn, Object[] startSplit, Object[] endSplit)
			throws SQLException {
		PreparedStatements ps = preparedStatementsMap.get(conn);
		if (ps == null) {
			ps = new PreparedStatements(conn);
			preparedStatementsMap.put(conn, ps);
		}
		return ps.getRowsForSplit(startSplit, endSplit);
	}

	protected static String mkString(Object[] arr, String delim) {
		if (arr.length == 0) {
			return "";
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < arr.length - 1; i++) {
			sb.append(arr[i]);
			sb.append(delim);
		}
		sb.append(arr[arr.length - 1]);
		return sb.toString();
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost/chute_test"
				// + "?profileSQL=true"
						, "root", "test");
		MySqlTableSchema schema = MySqlTableSchema.readTableSchemaFromConnection(
				conn, "chute_test", "testa");
		MySqlFullDumper dumper = new MySqlFullDumper(schema, 2);
		Iterator<Object[]> splitIt = dumper.createSplitPointIterator(conn);
		System.out.println("Splits:");
		Object[] previousSplitPoint = null;
		while (splitIt.hasNext()) {
			Object[] splitPoint = splitIt.next();
			System.out.println("End key: " + mkString(splitPoint, ":"));
			System.out.println("In split:");
			Iterator<Object[]> rowIt = dumper.getRowsForSplit(conn, previousSplitPoint, splitPoint);
			while (rowIt.hasNext()) {
				Object[] row = rowIt.next();
				System.out.println("> " + mkString(row, ":"));
			}
			previousSplitPoint = splitPoint;
		}
		System.out.println("Final: ");
		System.out.println("In split:");
		Iterator<Object[]> rowIt = dumper.getRowsForSplit(conn, previousSplitPoint, null);
		while (rowIt.hasNext()) {
			Object[] row = rowIt.next();
			System.out.println("> " + mkString(row, ":"));
		}
	}
}
