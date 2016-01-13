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
