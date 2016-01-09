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

import io.rodeo.chute.Row;
import io.rodeo.chute.StreamPosition;
import io.rodeo.chute.StreamProcessor;
import io.rodeo.chute.TableSchema;

import java.util.concurrent.ConcurrentHashMap;

public class CountPrintingStreamProcessor implements StreamProcessor {
	private final int batchSize;
	
	public CountPrintingStreamProcessor(int batchSize) {
		this.batchSize = batchSize;
	}
	
	public ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();
	
	@Override
	public void process(TableSchema schema, Row oldRow, Row newRow,
			StreamPosition pos) {
		MySqlTableSchema mySqlSchema = (MySqlTableSchema) schema;
		String key = mySqlSchema.getDatabaseName() + "." + mySqlSchema.getTableName() + "-" + (pos.isBackfill() ? "backfill" : "iterative");
		
		// Yeah, there's a race here, but we don't need exact counts
		Integer val = countMap.get(key);
		if (val == null) {
			val = new Integer(1);
		} else if (val >= batchSize) {
			System.out.println("Batch " + key + " hit batch size");
			val = new Integer(1);
		} else {
			val = new Integer(val.intValue() + 1);
		}
		countMap.put(key, val);
	}
}
