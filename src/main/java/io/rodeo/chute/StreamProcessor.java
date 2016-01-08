package io.rodeo.chute;

public interface StreamProcessor {
	public void process(TableSchema schema, Row oldRow, Row newRow, StreamPosition pos);
}
