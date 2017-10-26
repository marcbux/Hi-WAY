package de.huberlin.wbi.hiway.scheduler;

public class Runtime {
	public final double timestamp;
	public final double runtime;

	public Runtime(double timestamp, double runtime) {
		this.timestamp = timestamp;
		this.runtime = runtime;
	}

	@Override
	public String toString() {
		return timestamp + "," + runtime;
	}
}
