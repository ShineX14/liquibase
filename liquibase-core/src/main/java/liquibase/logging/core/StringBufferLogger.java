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
	
	public static String getLog(int start) {
		int length = loggerBuffer.length();
		if (start > length) {
		    return "";
		}
		return loggerBuffer.substring(start, length);
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
