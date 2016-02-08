package io.rodeo.chute;

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

public interface StreamProcessor {
	// Accepts a row delta. May be called from multiple threads.
	// May be called with the same StreamPosition multiple
	// times - should handle deduplication.
	public void process(TableSchema schema, Row oldRow, Row newRow,
			StreamPosition pos);
}
