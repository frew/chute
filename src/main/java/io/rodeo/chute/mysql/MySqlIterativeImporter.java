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

import io.rodeo.chute.PrintingStreamProcessor;
import io.rodeo.chute.Row;
import io.rodeo.chute.StreamProcessor;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySqlIterativeImporter implements EventListener, Runnable {
	private final String host;
	private final int port;
	private final String user;
	private final String password;

	private BinaryLogClient client;
	private MySqlStreamPosition position;
	private Connection schemaConn;
	private final List<StreamProcessor> processors;

	public MySqlIterativeImporter(String host, int port, String user,
			String password, MySqlStreamPosition position,
			Connection schemaConn, List<StreamProcessor> processors) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.position = position;
		this.schemaConn = schemaConn;
		this.processors = processors;
	}

	@Override
	public void run() {
		client = new BinaryLogClient(host, port, user, password);
		// TODO: GTID support
		client.setBinlogFilename(position.getFilename());
		client.setBinlogPosition(position.getOffset());
		client.registerEventListener(this);
		try {
			client.connect();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	// Map from table ID to schema
	private Map<Long, MySqlTableSchema> schemaMap = new HashMap<Long, MySqlTableSchema>();

	private void coerceLogRowTypes(MySqlTableSchema schema, Row logRow) {
		if (logRow == null) {
			return;
		}

		Object[] values = logRow.getValues();
		if (schema.getColumns().length != values.length) {
			throw new RuntimeException("Schema length doesn't match row length");
		}
		for (int i = 0; i < values.length; i++) {
			switch (schema.getColumns()[i].getColumnType()) {
			case INT:
				// TODO: Do we need to do coercion within integral types?
				break;
			case STRING:
				if (!(values[i] instanceof byte[])) {
					throw new RuntimeException(
							"Expected string to be represented as a byte array");
				}
				// TODO: Character sets and stuff
				values[i] = new String((byte[]) values[i]);
				break;
			default:
				throw new RuntimeException("Unknown schema type: "
						+ schema.getColumns()[i].getColumnType());
			}
		}
	}

	private void processRowChange(Long tableId,
			BitSet includedColsBeforeUpdate, BitSet includedCols, Row oldRow,
			Row newRow) {
		position.setFilename(client.getBinlogFilename());
		position.setOffset(client.getBinlogPosition());

		MySqlTableSchema schema = schemaMap.get(tableId);
		if (schema == null) {
			// TODO: Where do exceptions in the event loop go?
			throw new IllegalStateException("No table schema entry for "
					+ tableId);
		}
		String database = schema.getDatabaseName();
		String table = schema.getTableName();

		// Enforce that all of the row is there or not.
		// This is default in MySQL.
		// TODO: Add support for other modes.
		if (includedColsBeforeUpdate != null
				&& includedColsBeforeUpdate.cardinality() != includedColsBeforeUpdate
						.length()) {
			throw new IllegalStateException(
					"Expected all columns to be included");
		}
		if (includedCols != null
				&& includedCols.cardinality() != includedCols.length()) {
			throw new IllegalStateException(
					"Expected all columns to be included");
		}
		coerceLogRowTypes(schema, oldRow);
		coerceLogRowTypes(schema, newRow);

		for (StreamProcessor processor : processors) {
			processor.process(schema, oldRow, newRow, position);
		}
		// System.out.println("RC " + database + "." + table + " -> " + oldRow +
		// " : " + newRow);
	}

	@Override
	public void onEvent(Event event) {
		switch (event.getHeader().getEventType()) {
		case EXT_WRITE_ROWS:
			WriteRowsEventData wrEvent = (WriteRowsEventData) event.getData();
			for (Serializable[] row : wrEvent.getRows()) {
				processRowChange(wrEvent.getTableId(), null,
						wrEvent.getIncludedColumns(), null, new Row(row));
			}
			break;
		case EXT_UPDATE_ROWS:
			UpdateRowsEventData urEvent = (UpdateRowsEventData) event.getData();
			for (Entry<Serializable[], Serializable[]> rowChange : urEvent
					.getRows()) {
				processRowChange(urEvent.getTableId(),
						urEvent.getIncludedColumnsBeforeUpdate(),
						urEvent.getIncludedColumns(),
						new Row(rowChange.getKey()),
						new Row(rowChange.getValue()));
			}
			break;
		case EXT_DELETE_ROWS:
			DeleteRowsEventData drEvent = (DeleteRowsEventData) event.getData();
			for (Serializable[] row : drEvent.getRows()) {
				processRowChange(drEvent.getTableId(),
						drEvent.getIncludedColumns(), null, new Row(row), null);
			}
			break;
		case TABLE_MAP:
			TableMapEventData tmData = (TableMapEventData) event.getData();
			if (!schemaMap.containsKey(tmData.getTableId())) {
				MySqlTableSchema schema;
				try {
					schema = MySqlTableSchema
							.readTableSchemaFromConnection(schemaConn,
									tmData.getDatabase(), tmData.getTable());
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}

				// TODO: Check column types vs. schema
				if (schema.getColumns().length != tmData.getColumnTypes().length) {
					throw new RuntimeException("Got table map event " + tmData
							+ " that doesn't match retrieved schema " + schema);
				}

				System.out.println("Adding schema map data for "
						+ tmData.getTableId() + " -> " + schema);
				schemaMap.put(tmData.getTableId(), schema);
			}
			break;
		default:
			// System.out.println("Not handling " + event);
			break;
		}
	}

	public static void main(String[] args) throws IOException, SQLException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://localhost/chute_test"
				// + "?profileSQL=true"
				, "root", "test");

		MySqlStreamPosition pos = new MySqlStreamPosition(0, "", 4);
		List<StreamProcessor> processors = new ArrayList<StreamProcessor>();
		processors.add(new PrintingStreamProcessor());
		MySqlIterativeImporter importer = new MySqlIterativeImporter(
				"localhost", 3306, "root", "test", pos, conn, processors);
		importer.run();
	}
}
