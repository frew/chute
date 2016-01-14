package io.rodeo.chute.mysql;

import io.rodeo.chute.Key;
import io.rodeo.chute.Split;
import io.rodeo.chute.StreamProcessor;
import io.rodeo.chute.mysql.MySqlImportManager.SplitFullImportState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

class MySqlFullTableImportManager implements Runnable {
	public final MySqlTableSchema schema;
	public final Map<Split, SplitFullImportState> fullImportStates;
	public final MySqlFullSplitImporter importer;
	public final StreamProcessor processor;
	private ConnectionManager connManager;
	private final Semaphore concurrentImportSemaphore;
	private final int epoch;

	public MySqlFullTableImportManager(MySqlTableSchema schema,
			StreamProcessor processor, ConnectionManager connManager,
			Semaphore concurrentImportSemaphore, int epoch, int batchSize) {
		this.schema = schema;
		this.fullImportStates = new HashMap<Split, SplitFullImportState>();
		this.importer = new MySqlFullSplitImporter(schema, batchSize);
		this.processor = processor;
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
		System.out.println("Initialized " + fullImportStates.size()
				+ " splits");
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
			final int epoch, StreamProcessor processor) throws SQLException {
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
				processor, doneCb);
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
				startFullImport(split, conn, epoch, processor);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}