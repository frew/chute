package io.rodeo.chute;

import io.rodeo.chute.bigquery.BigQueryExportManager;
import io.rodeo.chute.mysql.CountPrintingStreamProcessor;
import io.rodeo.chute.mysql.MySqlImportManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class ChuteMain {
	private static final String CONFIG_FILENAME = "chute.yaml";

	@SuppressWarnings("unchecked")
	private static Map<String, Object> getMapping(Object mappingObj, String name) {
		if (!(mappingObj instanceof Map)) {
			throw new IllegalArgumentException("Expected " + name
					+ " to contain a mapping, but found"
					+ mappingObj.getClass().getCanonicalName());
		}
		return (Map<String, Object>) mappingObj;
	}

	private static String getString(Object stringObj, String name) {
		if (!(stringObj instanceof String)) {
			throw new IllegalArgumentException("Expected " + name
					+ " to be a string, but found "
					+ stringObj.getClass().getCanonicalName());
		}
		return (String) stringObj;
	}

	private static int getInt(Object intObj, String name) {
		if (!(intObj instanceof Integer)) {
			throw new IllegalArgumentException("Expected " + name
					+ " to be an int, but found "
					+ intObj.getClass().getCanonicalName());
		}
		return (Integer) intObj;
	}

	public static void main(String[] args) throws SQLException,
			FileNotFoundException {
		InputStream is = new FileInputStream(new File(CONFIG_FILENAME));
		Yaml yaml = new Yaml(new SafeConstructor());
		Map<String, Object> config = getMapping(yaml.load(is), CONFIG_FILENAME);
		Map<String, Object> importerConfigs = getMapping(
				config.get("importers"), "importers");
		System.out.println("Got " + importerConfigs.size() + " importers");

		Map<String, ImportManager> importers = processImporterConfigs(importerConfigs);

		Map<String, Object> exporterConfigs = getMapping(
				config.get("exporters"), "exporters");
		System.out.println("Got " + exporterConfigs.size() + " exporters");

		Map<String, ExportManager> exporters = processExporterConfigs(exporterConfigs);

		for (ImportManager importManager : importers.values()) {
			importManager.start();
		}
		/*
		 * ConnectionManager connManager = new ConnectionManager();
		 * 
		 * StreamProcessor processor = new CountPrintingStreamProcessor(1000);
		 * int currentEpoch = 0; MySqlImportManager manager = new
		 * MySqlImportManager(schemas, processor, currentEpoch, connManager);
		 * manager.start(); System.out.println("Importers started");
		 */
	}

	private static Map<String, ExportManager> processExporterConfigs(
			Map<String, Object> exporterConfigs) {
		Map<String, ExportManager> exporters = new HashMap<String, ExportManager>();
		for (Entry<String, Object> exporterConfigEntry : exporterConfigs
				.entrySet()) {
			String exporterId = exporterConfigEntry.getKey();
			Map<String, Object> exporterConfig = getMapping(
					exporterConfigEntry.getValue(), exporterId);
			String exporterType = getString(exporterConfig.get("type"),
					exporterId + ".type");
			switch (exporterType) {
			case "bigquery":
				String applicationName = getString(
						exporterConfig.get("application_name"), exporterId
								+ ".application_name");
				String projectId = getString(exporterConfig.get("project_id"),
						exporterId + ".project_id");
				String datasetId = getString(exporterConfig.get("dataset_id"),
						exporterId + ".dataset_id");
				exporters.put(exporterId, new BigQueryExportManager(
						applicationName, projectId, datasetId));
				break;
			default:
				throw new IllegalArgumentException("Exporter " + exporterId
						+ " has unknown type " + exporterType);
			}
		}
		return exporters;
	}

	private static Map<String, ImportManager> processImporterConfigs(
			Map<String, Object> importerConfigs) {
		Map<String, ImportManager> importers = new HashMap<String, ImportManager>();
		for (Entry<String, Object> importerConfigEntry : importerConfigs
				.entrySet()) {
			String importerId = importerConfigEntry.getKey();
			Map<String, Object> importerConfig = getMapping(
					importerConfigEntry.getValue(), importerId);
			String importerType = getString(importerConfig.get("type"),
					importerId + ".type");
			switch (importerType) {
			case "mysql":
				int epoch = getInt(importerConfig.get("epoch"), importerId
						+ ".epoch");
				String host = getString(importerConfig.get("host"), importerId
						+ ".host");
				int port = getInt(importerConfig.get("port"), importerId
						+ ".port");
				String user = getString(importerConfig.get("user"), importerId
						+ ".user");
				String password = getString(importerConfig.get("password"),
						importerId + ".password");
				String database = getString(importerConfig.get("database"),
						importerId + ".database");
				int batchSize = getInt(importerConfig.get("batch_size"),
						importerId + ".batch_size");
				int concurrentFullImports = getInt(
						importerConfig.get("concurrent_full_imports"),
						importerId + ".concurrent_full_imports");
				importers.put(importerId, new MySqlImportManager(
						new CountPrintingStreamProcessor(1000), epoch, host,
						port, user, password, database, batchSize,
						concurrentFullImports));
				break;
			default:
				throw new IllegalArgumentException("Importer " + importerId
						+ " has unknown type " + importerType);
			}
		}
		return importers;
	}
}
