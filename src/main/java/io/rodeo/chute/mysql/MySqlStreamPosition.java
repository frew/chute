package io.rodeo.chute.mysql;

import io.rodeo.chute.StreamPosition;

// TODO: gtid
public class MySqlStreamPosition implements StreamPosition {
	private final boolean backfill;
	private String filename;
	private long offset;

	public MySqlStreamPosition(boolean backfill) {
		this.backfill = backfill;
		if (backfill) {
			this.filename = null;
			this.offset = -1;
		} else {
			this.filename = "";
			this.offset = 4;
		}
	}

	public MySqlStreamPosition(String filename, long offset) {
		this.backfill = false;
		this.filename = filename;
		this.offset = offset;
	}

	@Override
	public boolean isBackfill() {
		return this.backfill;
	}

	@Override
	public String getPosition() {
		// TODO: Figure out formatting
		return "";
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
