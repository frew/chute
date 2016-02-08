package io.rodeo.chute;

import io.rodeo.chute.bigquery.BigQueryExporterConfiguration;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = BigQueryExporterConfiguration.class, name = "bigquery") })
public abstract class ExporterConfiguration {
	public abstract Exporter createExporter();
}
