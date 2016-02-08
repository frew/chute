package io.rodeo.chute;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ChuteConfiguration {
	@JsonProperty("importers")
	public Map<String, ImporterConfiguration> importerConfigurations;

	@JsonProperty("exporters")
	public Map<String, ExporterConfiguration> exporterConfigurations;

	@JsonProperty("connections")
	public Map<String, ConnectionConfiguration> connectionConfigurations;
}
