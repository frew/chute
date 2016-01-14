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

import io.rodeo.chute.StreamProcessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class MySqlImportManager {
	public static enum SplitFullImportState {
		NOT_STARTED, RUNNING, DONE
	}

	private static final int BATCH_SIZE = 10000;
	private static final int SIMULTANEOUS_FULL_IMPORTS = 5;

	private final Semaphore activeFullImports = new Semaphore(
			SIMULTANEOUS_FULL_IMPORTS);

	private final ConnectionManager connManager;
	private final StreamProcessor processor;
	private final int epoch;
	private final List<MySqlFullTableImportManager> fullImportManagers;

	public MySqlImportManager(List<MySqlTableSchema> schemas,
			StreamProcessor processor, int epoch, ConnectionManager connManager) {
		this.fullImportManagers = new ArrayList<MySqlFullTableImportManager>();
		this.processor = processor;
		this.epoch = epoch;
		this.connManager = connManager;
		for (MySqlTableSchema schema : schemas) {
			this.fullImportManagers.add(new MySqlFullTableImportManager(schema,
					processor, connManager, activeFullImports, epoch, BATCH_SIZE));
		}

	}

	public void start() throws SQLException {
		Connection itConn = connManager.createConnection();
		MySqlStreamPosition pos = new MySqlStreamPosition(epoch, "", 4);
		MySqlIterativeImporter importer = new MySqlIterativeImporter(
				"localhost", 3306, "root", "test", pos, itConn, processor);
		new Thread(importer).start();

		for (MySqlFullTableImportManager manager : fullImportManagers) {
			new Thread(manager).start();
		}
	}

	public static void main(String[] args) throws SQLException {
		ConnectionManager connManager = new ConnectionManager();
		Connection schemaConn = connManager.createConnection();
		List<String> tables = MySqlTableSchema.readTablesFromConnection(
				schemaConn, "chute_test");
		List<MySqlTableSchema> schemas = new ArrayList<MySqlTableSchema>();
		for (String tableName : tables) {
			schemas.add(MySqlTableSchema.readTableSchemaFromConnection(
					schemaConn, "chute_test", tableName));
		}
		StreamProcessor processor = new CountPrintingStreamProcessor(1000);
		int currentEpoch = 0;
		MySqlImportManager manager = new MySqlImportManager(schemas, processor,
				currentEpoch, connManager);
		manager.start();
		System.out.println("Importers started");
	}
}
