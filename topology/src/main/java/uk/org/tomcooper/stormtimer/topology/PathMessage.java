package uk.org.tomcooper.stormtimer.topology;

public class PathMessage {

	public String getMessageID() {
		return messageID;
	}

	public long getOriginTimestamp() {
		return originTimestamp;
	}

	public String[] getPath() {
		return path;
	}

	private String messageID;
	private long originTimestamp;
	private long entryNanoTimestamp;
	private long entryMilliTimestamp;
	private double stormNanoLatencyMs;
	private double stormMilliLatencyMs;
	private String[] path;

	public void setMessageID(String messageID) {
		this.messageID = messageID;
	}

	public void setOriginTimestamp(long originTimestamp) {
		this.originTimestamp = originTimestamp;
	}

	public void setPath(String[] path) {
		this.path = path;
	}

	public void addPathElement(String pathElement) {

		String[] oldPath = getPath();
		String[] newPath = new String[oldPath.length + 1];
		System.arraycopy(oldPath, 0, newPath, 0, oldPath.length);
		newPath[oldPath.length] = pathElement;
		setPath(newPath);

	}

	public double getStormNanoLatencyMs() {
		return stormNanoLatencyMs;
	}

	public void setStormNanoLatencyMs(double stormNanoLatencyMs) {
		this.stormNanoLatencyMs = stormNanoLatencyMs;
	}

	public double getStormMilliLatencyMs() {
		return stormMilliLatencyMs;
	}

	public void setStormMilliLatencyMs(double stormMilliLatencyMs) {
		this.stormMilliLatencyMs = stormMilliLatencyMs;
	}

	public long getEntryNanoTimestamp() {
		return entryNanoTimestamp;
	}

	public void setEntryNanoTimestamp(long entryNanoTimestamp) {
		this.entryNanoTimestamp = entryNanoTimestamp;
	}

	public long getEntryMilliTimestamp() {
		return entryMilliTimestamp;
	}

	public void setEntryMilliTimestamp(long entryMilliTimestamp) {
		this.entryMilliTimestamp = entryMilliTimestamp;
	}
}