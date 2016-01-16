package io.rodeo.chute.bigquery;

import java.io.IOException;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;

import io.rodeo.chute.ExportManager;

public class BigQueryExportManager implements ExportManager, Runnable {
	private final String applicationName;
	private final String projectId;
	private final String datasetId;

	public BigQueryExportManager(String applicationName, String projectId,
			String datasetId) {
		this.applicationName = applicationName;
		this.projectId = projectId;
		this.datasetId = datasetId;
	}

	@Override
	public void run() {

	}

	@Override
	public void start() {
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
		Bigquery bq = new Bigquery.Builder(transport, jsonFactory, credential)
				.setApplicationName(applicationName).build();
		try {
			bq.tables().list(projectId, datasetId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
