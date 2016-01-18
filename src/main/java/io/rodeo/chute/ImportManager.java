package io.rodeo.chute;

public interface ImportManager {
	public void start();

	// Must be called before start()
	public void addProcessor(StreamProcessor processor);
}
