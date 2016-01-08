package io.rodeo.chute;

// Represents an abstract position in a replication stream.
public interface StreamPosition {
	// True if this is a backfill (old data being sent with the stream)
	public boolean isBackfill();

	// An opaque string that sorts positions within the replication stream
	public String getPosition();
}
