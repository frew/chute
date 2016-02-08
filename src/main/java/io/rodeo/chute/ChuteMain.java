package io.rodeo.chute;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ChuteMain {
	private static final String CONFIG_FILENAME = "chute.yaml";

	public static void main(String[] args) throws SQLException,
			JsonParseException, JsonMappingException, IOException {
		InputStream is = new FileInputStream(new File(CONFIG_FILENAME));
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
		ChuteConfiguration config = mapper.readValue(is,
				ChuteConfiguration.class);
		Map<String, Importer> importManagers = new HashMap<String, Importer>(
				config.importerConfigurations.size());
		for (Entry<String, ImporterConfiguration> importerConfig : config.importerConfigurations
				.entrySet()) {
			importManagers.put(importerConfig.getKey(), importerConfig
					.getValue().createImporter());
		}
		Map<String, Exporter> exportManagers = new HashMap<String, Exporter>(
				config.exporterConfigurations.size());
		for (Entry<String, ExporterConfiguration> exporterConfig : config.exporterConfigurations
				.entrySet()) {
			exportManagers.put(exporterConfig.getKey(), exporterConfig
					.getValue().createExporter());
		}
		for (Entry<String, ConnectionConfiguration> connectionConfig : config.connectionConfigurations
				.entrySet()) {
			importManagers.get(connectionConfig.getValue().in).addProcessor(
					exportManagers.get(connectionConfig.getValue().out));
		}

		for (Entry<String, Exporter> exportManager : exportManagers
				.entrySet()) {
			exportManager.getValue().start();
		}

		for (Entry<String, Importer> importManager : importManagers
				.entrySet()) {
			importManager.getValue().start();
		}
	}
}
