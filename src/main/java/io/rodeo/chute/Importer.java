package io.rodeo.chute;

public interface Importer {
	public void start();

	// Must be called before start()
	public void addProcessor(StreamProcessor processor);
}
