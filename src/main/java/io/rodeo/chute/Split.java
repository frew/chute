package io.rodeo.chute;

public class Split {
	private final Key startKey;
	private final Key endKey;
	
	public Split(Key startKey, Key endKey) {
		this.startKey = startKey;
		this.endKey = endKey;
	}
	
	public Key getStartKey() {
		return startKey;
	}
	
	public Key getEndKey() {
		return endKey;
	}
}
