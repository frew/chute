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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class MySqlImportManager implements ImportManager {
	public static enum SplitFullImportState {
		NOT_STARTED, RUNNING, DONE
	}

	private final JdbcConnectionManager connManager;
	private final List<StreamProcessor> processors;
	private final int epoch;
	private final List<MySqlFullTableImportManager> fullImportManagers;
	private final String host;
	private final int port;
	private final String user;
	private final String password;
	private final String database;
	private final int batchSize;
	private final int concurrentFullImports;

	private final Semaphore activeFullImports;

	public MySqlImportManager(int epoch, String host, int port, String user,
			String password, String database, int batchSize,
			int concurrentFullImports) {
		this.fullImportManagers = new ArrayList<MySqlFullTableImportManager>();
		this.processors = new ArrayList<StreamProcessor>();
		this.epoch = epoch;
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.database = database;
		this.batchSize = batchSize;
		this.concurrentFullImports = concurrentFullImports;
		this.activeFullImports = new Semaphore(this.concurrentFullImports);
		this.connManager = new JdbcConnectionManager(host, port, user,
				password, database);
	}

	@Override
	public void start() {
		Connection schemaConn;
		try {
			schemaConn = connManager.createConnection();
			List<String> tables = MySqlTableSchema.readTablesFromConnection(
					schemaConn, database);
			List<MySqlTableSchema> schemas = new ArrayList<MySqlTableSchema>();
			for (String tableName : tables) {
				schemas.add(MySqlTableSchema.readTableSchemaFromConnection(
						schemaConn, database, tableName));
			}
			for (MySqlTableSchema schema : schemas) {
				this.fullImportManagers.add(new MySqlFullTableImportManager(
						schema, processors, connManager, activeFullImports,
						epoch, batchSize));
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
		MySqlStreamPosition pos = new MySqlStreamPosition(epoch, "", 4);
		MySqlIterativeImporter importer = new MySqlIterativeImporter(host,
				port, user, password, pos, itConn, processors);
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
