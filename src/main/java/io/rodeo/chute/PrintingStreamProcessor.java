package io.rodeo.chute;

public class PrintingStreamProcessor implements StreamProcessor {

	@Override
	public void process(TableSchema schema, Row oldRow, Row newRow,
			StreamPosition pos) {
		if (true) throw new RuntimeException("blash");
		System.out.println("Got " + schema + ": " + oldRow + " -> " + newRow + " @ " + pos);
	}

}
