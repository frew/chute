package io.rodeo.chute;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

// Figure out key partitioning strategy - COUNT DISTINCT vs COUNT vs something else

public class MySqlImporter {
	private BinaryLogFileReader reader;
	
	public MySqlImporter() {	
	}
		
	public void open() throws IOException {
		File binLogFile = new File("/usr/local/mysql/data/mysql-bin.000001");
		reader = new BinaryLogFileReader(binLogFile);
	}
	
	public Event readEvent() throws IOException {
		try {
			Event event = reader.readEvent();
			if (event == null) {
				throw new EOFException();
			}
			System.out.println(event);
			return event;
		} catch (IOException e) {
			try {
				reader.close();
			} catch (IOException e1) {
			}
			throw e;
		}
	}
	
	private Map<Long, TableMapEventData> tableMap = new HashMap<Long, TableMapEventData>();
	
	private void processRowChange(Long tableId, BitSet includedColsBeforeUpdate, BitSet includedCols, Serializable[] oldRow, Serializable[] newRow) throws IOException {
		TableMapEventData tmEvent = tableMap.get(tableId);
		if (tmEvent == null) {
			throw new IOException("No table map entry for " + tableId);
		}
		String database = tmEvent.getDatabase();
		String table = tmEvent.getTable();
	}
	
	public void parseEvent(Event event) throws IOException {
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
		MySqlImporter importer = new MySqlImporter();
		importer.open();
		while (true) {
			importer.parseEvent(importer.readEvent());
		}
	}
}
