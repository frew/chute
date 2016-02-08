package io.rodeo.chute;

import io.rodeo.chute.mysql.MySqlImporterConfiguration;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = MySqlImporterConfiguration.class, name = "mysql") })
public abstract class ImporterConfiguration {
	public abstract ImportManager createImporter();
}
