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
import io.rodeo.chute.PrintingStreamProcessor;
import io.rodeo.chute.StreamProcessor;
import io.rodeo.chute.Split;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

public class MySqlImportManager {
	public static enum SplitFullImportState {
		NOT_STARTED,
		RUNNING,
		DONE
	}

	private static final int BATCH_SIZE = 10000;
	private static final int SIMULTANEOUS_FULL_IMPORTS = 5;

	private final Semaphore activeFullImports = new Semaphore(SIMULTANEOUS_FULL_IMPORTS);

	private final ConnectionManager connManager;
	private final StreamProcessor processor;
	private final List<MySqlFullTableImportManager> fullImportManagers;

	private class MySqlFullTableImportManager implements Runnable {
		public final MySqlTableSchema schema;
		public final Map<Split, SplitFullImportState> fullImportStates;
		public MySqlFullImporter importer;
		public StreamProcessor processor;
		private ConnectionManager connManager;

		public MySqlFullTableImportManager(MySqlTableSchema schema, StreamProcessor processor,
				ConnectionManager connManager) {
			this.schema = schema;
			this.fullImportStates = new HashMap<Split, SplitFullImportState>();
			this.importer = new MySqlFullImporter(schema, BATCH_SIZE);
			this.processor = processor;
			this.connManager = connManager;
		}

		private void initializeSplits(Connection conn) throws SQLException {
			System.out.println("Initializing splits");
			Iterator<Key> splitIt = importer.createSplitPointIterator(conn);
			Key previousSplitPoint = null;
			while (splitIt.hasNext()) {
				Key splitPoint = splitIt.next();
				fullImportStates.put(
						new Split(previousSplitPoint, splitPoint), SplitFullImportState.NOT_STARTED);
			}
			fullImportStates.put(new Split(previousSplitPoint, null), SplitFullImportState.NOT_STARTED);
			System.out.println("Initialized " + fullImportStates.size() + " splits");
		}

		private Split getSplitToRun() {
			for (Entry<Split, SplitFullImportState> splitEnt: fullImportStates.entrySet()) {
				if (splitEnt.getValue() == SplitFullImportState.NOT_STARTED) {
					return splitEnt.getKey();
				}
			}
			return null;
		}

		private void setFullImportRunning(Split split) {
			activeFullImports.acquireUninterruptibly();;
			fullImportStates.put(split, SplitFullImportState.RUNNING);
		}

		private void setFullImportDone(Split split) {
			fullImportStates.put(split, SplitFullImportState.DONE);
			activeFullImports.release();
		}

		private void startFullImport(final Split split, final Connection conn, StreamProcessor processor) throws SQLException {
			setFullImportRunning(split);
			Runnable doneCb = new Runnable() {
				@Override
				public void run() {
					try {
						connManager.returnConnection(conn);
					} catch (SQLException e) {
						// TODO: More intelligent error handling
						e.printStackTrace();
					}
					setFullImportDone(split);
				}
			};
			Runnable r = importer.createImporterRunnable(conn, split, processor, doneCb);
			new Thread(r).start();
		}

		@Override
		public void run() {
			try {
				Connection splitConn = connManager.createConnection();
				initializeSplits(splitConn);
				connManager.returnConnection(splitConn);
				Split split;

				while ((split = getSplitToRun()) != null) {
					Connection conn = connManager.createConnection();
					startFullImport(split, conn, processor);
				}
			} catch (SQLException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	public MySqlImportManager(List<MySqlTableSchema> schemas, StreamProcessor processor, ConnectionManager connManager) {
		this.fullImportManagers = new ArrayList<MySqlFullTableImportManager>();
		this.processor = processor;
		this.connManager = connManager;
		for (MySqlTableSchema schema: schemas) {
			this.fullImportManagers.add(new MySqlFullTableImportManager(schema, processor, connManager));
		}

	}

	public void start() throws SQLException {
		Connection itConn = connManager.createConnection();
		MySqlStreamPosition pos = new MySqlStreamPosition(false);
		MySqlIterativeImporter importer = new MySqlIterativeImporter(
				"localhost", 3306, "root", "test", pos, itConn, processor);
		new Thread(importer).start();

		for (MySqlFullTableImportManager manager: fullImportManagers) {
			new Thread(manager).start();
		}
	}

	public static void main(String[] args) throws SQLException {
		ConnectionManager connManager = new ConnectionManager();
		Connection schemaConn = connManager.createConnection();
		List<String> tables = MySqlTableSchema.readTablesFromConnection(schemaConn, "chute_test");
		List<MySqlTableSchema> schemas = new ArrayList<MySqlTableSchema>();
		for (String tableName: tables) {
			schemas.add(
					MySqlTableSchema.readTableSchemaFromConnection(schemaConn, "chute_test", tableName));
		}
		StreamProcessor processor = new CountPrintingStreamProcessor(1000);
		MySqlImportManager manager = new MySqlImportManager(schemas, processor, connManager);
		manager.start();
		System.out.println("Importers started");
	}
}
