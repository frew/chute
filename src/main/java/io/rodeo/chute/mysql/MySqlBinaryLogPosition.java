package io.rodeo.chute.mysql;

public class MySqlBinaryLogPosition {
	private final boolean isGtid;

	// Non-GTID
	private String filename;
	private long offset;

	public MySqlBinaryLogPosition() {
		this.isGtid = false;

		// Special values for first file
		this.filename = "";
		this.offset = 4;
	}

	public MySqlBinaryLogPosition(String filename, long offset) {
		this.isGtid = false;
		this.filename = filename;
		this.offset = offset;
	}

	public boolean isGtid() {
		return isGtid;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getFilename() {
		return filename;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getOffset() {
		return offset;
	}
}
