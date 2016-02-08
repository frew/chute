package io.rodeo.chute.bigquery;

import io.rodeo.chute.Exporter;
import io.rodeo.chute.ExporterConfiguration;

public class BigQueryExporterConfiguration extends ExporterConfiguration {
	public String applicationName;
	public String projectId;
	public String datasetId;

	public Exporter createExporter() {
		return new BigQueryExporter(this);
	}
}
