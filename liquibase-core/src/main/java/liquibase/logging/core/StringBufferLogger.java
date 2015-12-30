package liquibase.logging.core;

import liquibase.logging.LogLevel;

public class StringBufferLogger extends DefaultLogger {

	private static StringBuffer loggerBuffer = new StringBuffer(1000000);
	private static boolean enabled = false;
	
	public static void enable() {
		enabled = true;
	}

	public static void disable() {
		enabled = false;
	}
	
	public static void reset() {
		loggerBuffer.setLength(0);
	}
	
	public static int getLogLength() {
		return loggerBuffer.length();
	}
	
	public static String getLog(int start, int end) {
		int logLimit = loggerBuffer.length();
		if (start > logLimit) {
			throw new IllegalArgumentException("Invalid start index " + start + " greater than log length " + logLimit);
		}
		if (end > logLimit) {
			throw new IllegalArgumentException("Invalid end index " + end + " greater than log length " + logLimit);
		}
		return loggerBuffer.substring(start, end);
	}
	
	@Override
	public int getPriority() {
		return super.getPriority() + 1;
	}

	@Override
	protected void print(LogLevel logLevel, String message) {
		super.print(logLevel, message);
		if (enabled) {
			loggerBuffer.append(message).append("\n");
		}
	}

}
