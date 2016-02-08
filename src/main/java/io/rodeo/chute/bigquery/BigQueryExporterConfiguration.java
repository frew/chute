package io.rodeo.chute.bigquery;

import io.rodeo.chute.ExportManager;
import io.rodeo.chute.ExporterConfiguration;

public class BigQueryExporterConfiguration extends ExporterConfiguration {
	public String applicationName;
	public String projectId;
	public String datasetId;

	public ExportManager createExporter() {
		return new BigQueryExportManager(this);
	}
}
