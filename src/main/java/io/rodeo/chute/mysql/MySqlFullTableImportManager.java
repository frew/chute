package io.rodeo.chute.mysql;

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

import io.rodeo.chute.Key;
import io.rodeo.chute.Split;
import io.rodeo.chute.StreamProcessor;
import io.rodeo.chute.mysql.MySqlImporter.SplitFullImportState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

class MySqlFullTableImportManager implements Runnable {
	public final MySqlTableSchema schema;
	public final Map<Split, SplitFullImportState> fullImportStates;
	public final MySqlFullSplitImporter importer;
	public final List<StreamProcessor> processors;
	private JdbcConnectionManager connManager;
	private final Semaphore concurrentImportSemaphore;
	private final int epoch;

	public MySqlFullTableImportManager(MySqlTableSchema schema,
			List<StreamProcessor> processors,
			JdbcConnectionManager connManager,
			Semaphore concurrentImportSemaphore, int epoch, int batchSize) {
		this.schema = schema;
		this.fullImportStates = new HashMap<Split, SplitFullImportState>();
		this.importer = new MySqlFullSplitImporter(schema, batchSize);
		this.processors = processors;
		this.connManager = connManager;
		this.concurrentImportSemaphore = concurrentImportSemaphore;
		this.epoch = epoch;
	}

	private void initializeSplits(Connection conn) throws SQLException {
		System.out.println("Initializing splits");
		Iterator<Key> splitIt = importer.createSplitPointIterator(conn);
		Key previousSplitPoint = null;
		while (splitIt.hasNext()) {
			Key splitPoint = splitIt.next();
			fullImportStates.put(new Split(previousSplitPoint, splitPoint),
					SplitFullImportState.NOT_STARTED);
		}
		fullImportStates.put(new Split(previousSplitPoint, null),
				SplitFullImportState.NOT_STARTED);
		System.out
				.println("Initialized " + fullImportStates.size() + " splits");
	}

	private Split getSplitToRun() {
		for (Entry<Split, SplitFullImportState> splitEnt : fullImportStates
				.entrySet()) {
			if (splitEnt.getValue() == SplitFullImportState.NOT_STARTED) {
				return splitEnt.getKey();
			}
		}
		return null;
	}

	private void setFullImportRunning(Split split) {
		concurrentImportSemaphore.acquireUninterruptibly();
		fullImportStates.put(split, SplitFullImportState.RUNNING);
	}

	private void setFullImportDone(Split split) {
		fullImportStates.put(split, SplitFullImportState.DONE);
		concurrentImportSemaphore.release();
	}

	private void startFullImport(final Split split, final Connection conn,
			final int epoch, List<StreamProcessor> processors)
			throws SQLException {
		setFullImportRunning(split);
		Runnable doneCb = new Runnable() {
			@Override
			public void run() {
				try {
					connManager.returnConnection(conn);
				} catch (SQLException e) {
					// TODO: More intelligent error handling
					e.printStackTrace();
				}
				setFullImportDone(split);
			}
		};
		Runnable r = importer.createImporterRunnable(conn, epoch, split,
				processors, doneCb);
		new Thread(r).start();
	}

	@Override
	public void run() {
		try {
			Connection splitConn = connManager.createConnection();
			initializeSplits(splitConn);
			connManager.returnConnection(splitConn);
			Split split;

			while ((split = getSplitToRun()) != null) {
				Connection conn = connManager.createConnection();
				startFullImport(split, conn, epoch, processors);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}