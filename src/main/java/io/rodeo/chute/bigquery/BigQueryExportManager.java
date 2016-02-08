package io.rodeo.chute.bigquery;

import io.rodeo.chute.ColumnSchema;
import io.rodeo.chute.ColumnType;
import io.rodeo.chute.Exporter;
import io.rodeo.chute.Row;
import io.rodeo.chute.StreamPosition;
import io.rodeo.chute.TableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;

public class BigQueryExportManager implements Exporter {
	private final BigQueryExporterConfiguration config;
	private final Set<String> checkedSchemas;
	private final Map<String, com.google.api.services.bigquery.model.TableSchema> existingSchemaMap;

	private final Bigquery bq;

	public BigQueryExportManager(BigQueryExporterConfiguration config) {
		this.config = config;
		this.checkedSchemas = new HashSet<String>();
		this.existingSchemaMap = new HashMap<String, com.google.api.services.bigquery.model.TableSchema>();

		HttpTransport transport = new NetHttpTransport();
		JsonFactory jsonFactory = new JacksonFactory();
		GoogleCredential credential;
		try {
			credential = GoogleCredential.getApplicationDefault(transport,
					jsonFactory);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (credential.createScopedRequired()) {
			credential = credential.createScoped(BigqueryScopes.all());
		}
		this.bq = new Bigquery.Builder(transport, jsonFactory, credential)
				.setApplicationName(this.config.applicationName).build();
	}

	@Override
	public void start() {
		try {
			String pageToken = null;
			do {
				Tables.List listReq = bq.tables().list(config.projectId,
						config.datasetId);
				if (pageToken != null) {
					listReq.setPageToken(pageToken);
				}
				TableList list = listReq.execute();
				pageToken = list.getNextPageToken();
				for (TableList.Tables table : list.getTables()) {
					String tableId = table.getTableReference().getTableId();
					Table tableResource = bq.tables()
							.get(config.projectId, config.datasetId, tableId)
							.execute();
					existingSchemaMap.put(tableId, tableResource.getSchema());
				}
			} while (pageToken != null);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String getBigQueryTypeForColumnType(ColumnType columnType) {
		switch (columnType) {
		case FLOAT:
			return "FLOAT";
		case INT:
			return "INTEGER";
		case STRING:
			return "STRING";
		default:
			throw new IllegalArgumentException("Unhandled column type: "
					+ columnType);
		}
	}

	private com.google.api.services.bigquery.model.TableSchema createBigQuerySchema(
			TableSchema schema) {
		List<TableFieldSchema> bqFields = new ArrayList<TableFieldSchema>();
		for (ColumnSchema columnSchema : schema.getColumns()) {
			TableFieldSchema fieldSchema = new TableFieldSchema();
			fieldSchema.setName(columnSchema.getColumnName());
			fieldSchema.setType(getBigQueryTypeForColumnType(columnSchema
					.getColumnType()));
			fieldSchema
					.setDescription("Automatically created by chute for table "
							+ schema.getTableId());
			bqFields.add(fieldSchema);
		}

		com.google.api.services.bigquery.model.TableSchema bqSchema = new com.google.api.services.bigquery.model.TableSchema();
		bqSchema.setFields(bqFields);
		return bqSchema;
	}

	private void checkSchema(TableSchema schema,
			com.google.api.services.bigquery.model.TableSchema bqSchema) {

	}

	@Override
	public void process(TableSchema schema, Row oldRow, Row newRow,
			StreamPosition pos) {
		if (!existingSchemaMap.containsKey(schema.getTableId())) {
			synchronized (this) {
				if (!existingSchemaMap.containsKey(schema.getTableId())) {
					System.out.println("Creating BQ schema for " + schema);
					com.google.api.services.bigquery.model.TableSchema bqSchema = createBigQuerySchema(schema);
					Table tableResource = new Table();
					tableResource.setId(schema.getTableId());
					tableResource.setSchema(bqSchema);
					TableReference reference = new TableReference();
					reference.setProjectId(config.projectId);
					reference.setDatasetId(config.datasetId);
					reference.setTableId(schema.getTableId());
					tableResource.setTableReference(reference);
					try {
						existingSchemaMap
								.put(schema.getTableId(),
										bq.tables()
												.insert(config.projectId,
														config.datasetId,
														tableResource)
												.execute().getSchema());
						checkedSchemas.add(schema.getTableId());
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
		} else if (existingSchemaMap.containsKey(schema.getTableId())
				&& !checkedSchemas.contains(schema.getTableId())) {
			synchronized (this) {
				if (!checkedSchemas.contains(schema.getTableId())) {
					checkSchema(schema,
							existingSchemaMap.get(schema.getTableId()));
					checkedSchemas.add(schema.getTableId());
				}
			}
		}
		try {
			TableDataInsertAllRequest req = new TableDataInsertAllRequest();
			List<Rows> rowsList = new ArrayList<Rows>();
			Rows rows = new Rows();
			Map<String, Object> rowJson = new HashMap<String, Object>();
			for (int i = 0; i < newRow.getValues().length; i++) {
				rowJson.put(schema.getColumns()[i].getColumnName(),
						newRow.getValues()[i]);
			}
			// TODO: Figure out if this is what we should be doing
			// rows.setInsertId(new String(pos.getPosition()));
			rows.setJson(rowJson);
			rowsList.add(rows);
			req.setRows(rowsList);
			bq.tabledata()
					.insertAll(config.projectId, config.datasetId,
							schema.getTableId(), req).execute();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
