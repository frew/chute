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

import io.rodeo.chute.ImportManager;
import io.rodeo.chute.StreamProcessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class MySqlImportManager implements ImportManager {
	public static enum SplitFullImportState {
		NOT_STARTED, RUNNING, DONE
	}

	private final MySqlImporterConfiguration config;
	private final JdbcConnectionManager connManager;
	private final List<StreamProcessor> processors;
	private final List<MySqlFullTableImportManager> fullImportManagers;

	private final Semaphore activeFullImports;

	public MySqlImportManager(MySqlImporterConfiguration config) {
		this.fullImportManagers = new ArrayList<MySqlFullTableImportManager>();
		this.processors = new ArrayList<StreamProcessor>();
		this.config = config;
		this.activeFullImports = new Semaphore(
				this.config.concurrentFullImports);
		this.connManager = new JdbcConnectionManager(config.host, config.port,
				config.user, config.password, config.database);
	}

	@Override
	public void start() {
		Connection schemaConn;
		try {
			schemaConn = connManager.createConnection();
			List<String> tables = MySqlTableSchema.readTablesFromConnection(
					schemaConn, config.database);
			List<MySqlTableSchema> schemas = new ArrayList<MySqlTableSchema>();
			for (String tableName : tables) {
				schemas.add(MySqlTableSchema.readTableSchemaFromConnection(
						schemaConn, config.database, tableName));
			}
			for (MySqlTableSchema schema : schemas) {
				this.fullImportManagers.add(new MySqlFullTableImportManager(
						schema, processors, connManager, activeFullImports,
						config.epoch, config.batchSize));
			}
			connManager.returnConnection(schemaConn);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		Connection itConn;
		try {
			itConn = connManager.createConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		MySqlStreamPosition pos = new MySqlStreamPosition(config.epoch, "", 4);
		MySqlIterativeImporter importer = new MySqlIterativeImporter(
				config.host, config.port, config.user, config.password, pos,
				itConn, processors);
		new Thread(importer).start();

		for (MySqlFullTableImportManager manager : fullImportManagers) {
			new Thread(manager).start();
		}
	}

	@Override
	public void addProcessor(StreamProcessor processor) {
		processors.add(processor);
	}
}
