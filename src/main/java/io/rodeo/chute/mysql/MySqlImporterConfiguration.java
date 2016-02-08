package io.rodeo.chute.mysql;

import io.rodeo.chute.ImportManager;
import io.rodeo.chute.ImporterConfiguration;

public class MySqlImporterConfiguration extends ImporterConfiguration {
	public String host;
	public int port;
	public String user;
	public String password;
	public String database;
	public int batchSize;
	public int epoch;
	public int concurrentFullImports;

	public ImportManager createImporter() {
		return new MySqlImportManager(this);
	}
}
