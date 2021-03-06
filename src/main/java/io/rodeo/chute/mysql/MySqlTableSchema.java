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

import io.rodeo.chute.ColumnSchema;
import io.rodeo.chute.ColumnType;
import io.rodeo.chute.TableSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MySqlTableSchema implements TableSchema {
	private final String databaseName;
	private final String tableName;
	private final ColumnSchema[] columns;
	private final int[] primaryKeyColumnOffsets;

	public MySqlTableSchema(String databaseName, String tableName,
			ColumnSchema[] columns, int[] primaryKeyColumnOffsets) {
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

	// TODO: Should probably have option to include database name as well
	@Override
	public String getTableId() {
		return getTableName();
	}

	@Override
	public ColumnSchema[] getColumns() {
		return columns;
	}

	@Override
	public int[] getPrimaryKeyColumnOffsets() {
		return primaryKeyColumnOffsets;
	}

	public String getCommaDelimitedPrimaryKeyColumns() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (int offset : primaryKeyColumnOffsets) {
			if (!first) {
				sb.append(", ");
			}
			sb.append(columns[offset].getColumnName());
			first = false;
		}
		return sb.toString();
	}

	public String getCommaDelimitedAllColumns() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (ColumnSchema column : columns) {
			if (!first) {
				sb.append(", ");
			}
			sb.append(column.getColumnName());
			first = false;
		}
		return sb.toString();
	}

	protected String getParameterList(int count) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (int i = 0; i < count; i++) {
			if (!first) {
				sb.append(", ");
			}
			sb.append("?");
			first = false;
		}
		return sb.toString();
	}

	public String getPrimaryKeyParameterList() {
		return getParameterList(primaryKeyColumnOffsets.length);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(databaseName);
		sb.append(".");
		sb.append(tableName);
		sb.append(", Cols: [");
		boolean first = true;
		for (ColumnSchema col : columns) {
			if (!first) {
				sb.append(", ");
			}
			sb.append(col);
			first = false;
		}
		sb.append("], PK: [");
		sb.append(getCommaDelimitedPrimaryKeyColumns());
		sb.append("]");
		return sb.toString();
	}

	public static ArrayList<String> readTablesFromConnection(
			java.sql.Connection conn, String database) throws SQLException {
		ResultSet tableRS = conn.getMetaData().getTables(null, database, null,
				new String[] { "TABLE" });
		ArrayList<String> tableNames = new ArrayList<String>();
		while (tableRS.next()) {
			String tableName = tableRS.getString(3);
			tableNames.add(tableName);
		}
		return tableNames;
	}

	public static MySqlTableSchema readTableSchemaFromConnection(
			java.sql.Connection conn, String database, String table)
			throws SQLException {
		ResultSet colRS = conn.getMetaData().getColumns(null, database, table,
				null);
		ArrayList<ColumnSchema> colSchemas = new ArrayList<ColumnSchema>();
		while (colRS.next()) {
			String colName = colRS.getString(4);
			String sqlType = colRS.getString(6); // MySQL type name - 5 is
													// generic type name
			int colSize = colRS.getInt(7);
			ColumnType type = null;
			switch (sqlType) {
			case "TINYINT":
				type = ColumnType.INT;
				colSize = 1;
				break;
			case "SMALLINT":
				type = ColumnType.INT;
				colSize = 1;
				break;
			case "MEDIUMINT":
				type = ColumnType.INT;
				colSize = 1;
				break;
			case "INT":
				type = ColumnType.INT;
				colSize = 4;
				break;
			case "BIGINT":
				type = ColumnType.INT;
				colSize = 8;
				break;
			case "VARCHAR":
				type = ColumnType.STRING;
				// colSize is set from db above
				break;
			default:
				throw new IllegalArgumentException("Unrecognized MySQL type: "
						+ sqlType);
			}
			ColumnSchema schema = new ColumnSchema(colName, type, colSize);
			colSchemas.add(schema);
			// String defaultVal = colRS.getString(13);
		}

		ResultSet indexRS = conn.getMetaData().getIndexInfo(null, database,
				table, true, false);
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
							"Couldn't find primary key index column name "
									+ colName + " in table " + database + "."
									+ table);
				}
			}
		}
		ColumnSchema[] colSchemaArr = colSchemas
				.toArray(new ColumnSchema[colSchemas.size()]);

		int[] primaryKeyIndexOffsetArr = new int[primaryKeyIndexOffsets.size()];
		for (int i = 0; i < primaryKeyIndexOffsets.size(); i++) {
			primaryKeyIndexOffsetArr[i] = primaryKeyIndexOffsets.get(i)
					.intValue();
		}
		MySqlTableSchema tableSchema = new MySqlTableSchema(database, table,
				colSchemaArr, primaryKeyIndexOffsetArr);
		return tableSchema;
	}

	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost/chute_test", "root", "test");
		List<String> tables = readTablesFromConnection(conn, "chute_test");
		for (String table : tables) {
			System.out.println("Got table: " + table);
			MySqlTableSchema schema = readTableSchemaFromConnection(conn,
					"chute_test", table);
			System.out.println("Schema: " + schema);
		}
	}
}