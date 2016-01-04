package io.rodeo.chute.mysql;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySqlIterativeImporter implements EventListener {
	private final String host;
	private final int port;
	private final String user;
	private final String password;
	
	private BinaryLogClient client;
	private MySqlBinaryLogPosition position;

	public MySqlIterativeImporter(String host, int port, String user, String password,
			MySqlBinaryLogPosition position) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.position = position;
	}
	
	public void run() throws IOException {
		client = new BinaryLogClient(host, port, user, password);
		client.setBinlogFilename(position.getFilename());
		client.setBinlogPosition(position.getOffset());
		client.registerEventListener(this);
		client.connect();
	}
	
	private Map<Long, TableMapEventData> tableMap = new HashMap<Long, TableMapEventData>();
	
	private void processRowChange(Long tableId, BitSet includedColsBeforeUpdate, BitSet includedCols, Serializable[] oldRow, Serializable[] newRow) {
		TableMapEventData tmEvent = tableMap.get(tableId);
		if (tmEvent == null) {
			throw new IllegalStateException("No table map entry for " + tableId);
		}
		String database = tmEvent.getDatabase();
		String table = tmEvent.getTable();
	}
	
	@Override
	public void onEvent(Event event) {
		switch (event.getHeader().getEventType()) {
		case EXT_WRITE_ROWS:
			WriteRowsEventData wrEvent = (WriteRowsEventData) event.getData();
			for (Serializable[] row: wrEvent.getRows()) {
				processRowChange(wrEvent.getTableId(), null, wrEvent.getIncludedColumns(), null, row);
			}
			break;
		case EXT_UPDATE_ROWS:
			UpdateRowsEventData urEvent = (UpdateRowsEventData) event.getData();
			for (Entry<Serializable[], Serializable[]> rowChange: urEvent.getRows()) {
				processRowChange(urEvent.getTableId(), urEvent.getIncludedColumnsBeforeUpdate(), urEvent.getIncludedColumns(), rowChange.getKey(), rowChange.getValue());
			}
			break;
		case EXT_DELETE_ROWS:
			DeleteRowsEventData drEvent = (DeleteRowsEventData) event.getData();
			for (Serializable[] row: drEvent.getRows()) {
				processRowChange(drEvent.getTableId(), drEvent.getIncludedColumns(), null, row, null);
			}
			break;
		case TABLE_MAP:
			TableMapEventData tmData = (TableMapEventData) event.getData();
			System.out.println("Adding table map data for " + tmData.getTableId() + " -> " + tmData.getTable());
			tableMap.put(tmData.getTableId(), tmData);
			break;
		default:
			System.out.println("Not handling " + event.getHeader().getEventType());
		}
	}
	
	public static void main(String[] args) throws IOException {
		MySqlBinaryLogPosition pos = new MySqlBinaryLogPosition();
		MySqlIterativeImporter importer = new MySqlIterativeImporter(
				"localhost", 3306, "root", "test", pos);
		importer.run();
	}
}
