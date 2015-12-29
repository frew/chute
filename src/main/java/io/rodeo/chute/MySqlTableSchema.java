package io.rodeo.chute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MySqlTableSchema {
	private String databaseName;
	private String tableName;
	private MySqlColumnSchema[] columns;
	private int[] primaryKeyColumnOffsets;
	
	public MySqlTableSchema(String databaseName, String tableName, MySqlColumnSchema[] columns, int[] primaryKeyColumnOffsets) {
		this.databaseName = databaseName;
		this.tableName = tableName;
		this.columns = columns;
		this.primaryKeyColumnOffsets = primaryKeyColumnOffsets;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public MySqlColumnSchema[] getColumns() {
		return columns;
	}

	public int[] getPrimaryKeyColumnOffsets() {
		return primaryKeyColumnOffsets;
	}
	
	@Override public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(databaseName);
		sb.append(".");
		sb.append(tableName);
		sb.append(", Cols: [");
		for (MySqlColumnSchema col: columns) {
			sb.append(col);
		}
		sb.append("], PK:[");
		for (int offset: primaryKeyColumnOffsets) {
			sb.append(columns[offset].getColumnName());
		}
		sb.append("]");
		return sb.toString();
	}
	
	public static ArrayList<String> readTablesFromConn(java.sql.Connection conn, String database) throws SQLException {
		ResultSet tableRS = conn.getMetaData().getTables(null, database, null, new String[]{"TABLE"});
		ArrayList<String> tableNames = new ArrayList<String>();
		while (tableRS.next()) {
			String tableName = tableRS.getString(3);
			tableNames.add(tableName);
		}
		return tableNames;
	}
	
	public static MySqlTableSchema readTableSchemaFromConn(java.sql.Connection conn, String database, String table) throws SQLException {
		ResultSet colRS = conn.getMetaData().getColumns(null, database, table, null);
		ArrayList<MySqlColumnSchema> colSchemas = new ArrayList<MySqlColumnSchema>();
		while (colRS.next()) {
			String colName = colRS.getString(4);
			// String sqlType = colRS.getString(6);  // MySQL type namae - 5 is generic type name
			int colSize = colRS.getInt(7);
			// TODO: Implement
			MySqlColumnSchema schema = new MySqlColumnSchema(colName, ColumnType.INT, colSize);
			colSchemas.add(schema);
			// String defaultVal = colRS.getString(13);
		}
		
		ResultSet indexRS = conn.getMetaData().getIndexInfo(null, database, table, true, false);
		ArrayList<Integer> primaryKeyIndexOffsets = new ArrayList<Integer>();
		while (indexRS.next()) {
			// System.out.println("IT: " + indexRS.getString(6));
			if ("PRIMARY".equals(indexRS.getString(6))) {
				String colName = indexRS.getString(9);
				int i;
				for (i = 0; i < colSchemas.size(); i++) {
					if (colName.equals(colSchemas.get(i).getColumnName())) {
						primaryKeyIndexOffsets.add(i);
						break;
					}
				}
				if (i == colSchemas.size()) {
					throw new IllegalStateException(
							"Couldn't find primary key index column name " + colName + 
							" in table " + database + "." + table);
				}
			}
		}
		MySqlColumnSchema[] colSchemaArr = colSchemas.toArray(new MySqlColumnSchema[colSchemas.size()]);
		
		int[] primaryKeyIndexOffsetArr = new int[primaryKeyIndexOffsets.size()];
		for (int i = 0; i < primaryKeyIndexOffsets.size(); i++) {
			primaryKeyIndexOffsetArr[i] = primaryKeyIndexOffsets.get(i).intValue();
		}
		MySqlTableSchema tableSchema = new MySqlTableSchema(database, table,
				colSchemaArr, primaryKeyIndexOffsetArr);
		return tableSchema;
	}
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/chute_test", "root", "test");
		List<String> tables = readTablesFromConn(conn, "chute_test");
		for (String table: tables) {
			System.out.println("Got table: " + table);
			MySqlTableSchema schema = readTableSchemaFromConn(conn, "chute_test", table);
			System.out.println("Schema: " + schema);
		}
	}
}