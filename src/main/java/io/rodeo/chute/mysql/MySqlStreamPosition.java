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

import java.nio.ByteBuffer;

import io.rodeo.chute.StreamPosition;

/*-
 * Represents a position in the replication stream. For MySQL there are
 * currently two possible kinds of positions:
 * - An iterative position in the replication stream of the form
 *   (epoch, filename, offset)
 *   The epoch is an integer that's incremented every time the
 *   master changes. The filename and offset are recorded as shown in
 *   "SHOW MASTER STATUS".
 *
 * TODO: Currently we don't support GTID yet.
 */
public class MySqlStreamPosition implements StreamPosition {
	// Incremented each time the server is changed
	private int epoch;

	private String filename;
	private long offset;

	public MySqlStreamPosition(int epoch, String filename, long offset) {
		this.epoch = epoch;
		this.filename = filename;
		this.offset = offset;
	}

	public MySqlStreamPosition(byte[] positionBytes) {
		if (positionBytes.length < 12) {
			throw new IllegalArgumentException(
					"MySQL position must be at least 16 bytes, but was "
							+ positionBytes.length);
		}
		ByteBuffer buf = ByteBuffer.wrap(positionBytes);
		this.epoch = buf.getInt();
		byte[] stringBytes = new byte[buf.remaining() - 8];
		buf.get(stringBytes);
		this.filename = new String(stringBytes);
		this.offset = buf.getLong();
	}

	@Override
	public byte[] getPosition() {
		byte[] fileBytes = filename.getBytes();
		int positionLength = 4 + fileBytes.length + 8;
		ByteBuffer buf = ByteBuffer.allocate(positionLength);
		buf.putInt(epoch);
		buf.put(fileBytes);
		buf.putLong(offset);
		byte[] retArray = new byte[positionLength];
		buf.flip();
		buf.get(retArray);
		return retArray;
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

	@Override
	public String toString() {
		return "MySqlStreamPosition: (" + epoch + ":" + filename + ":" + offset
				+ ")";
	}
}
