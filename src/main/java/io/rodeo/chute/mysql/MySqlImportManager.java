package io.rodeo.chute.mysql;

import io.rodeo.chute.Split;

import java.util.HashMap;
import java.util.Map;

public class MySqlImportManager {
	public static enum SplitFullImportState {
		NOT_STARTED,
		RUNNING,
		DONE
	}

	private class MySqlImportState {
		public final MySqlTableSchema schema;
		public final Map<Split, SplitFullImportState> fullImportStates;
		public boolean fullImportDone;
		
		
		public MySqlImportState(MySqlTableSchema schema) {
			this.schema = schema;
			this.fullImportStates = new HashMap<Split, SplitFullImportState>();
			this.fullImportDone = false;
		}
	}
	
	private final MySqlImportState state;
	
	public MySqlImportManager(MySqlTableSchema schema) {
		this.state = new MySqlImportState(schema);
	}
}
