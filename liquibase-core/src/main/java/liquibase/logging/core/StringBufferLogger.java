package liquibase.logging.core;

import liquibase.logging.LogLevel;

public class StringBufferLogger extends DefaultLogger {

  private static StringBuffer loggerBuffer = new StringBuffer(1000000);
  private static boolean enabled = false;
  private static boolean hasSevereLog = false;

  @Deprecated // kept for compatibility
  public static void enable() {}

  public static void reset() {
    loggerBuffer.setLength(0);
    hasSevereLog = false;
    enabled = true;
  }

  public static boolean hasSevereLog() {
    return hasSevereLog;
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

  @Override
  public void severe(String message) {
    super.severe(message);
    hasSevereLog = true;
  }

  @Override
  public void severe(String message, Throwable e) {
    super.severe(message, e);
    hasSevereLog = true;
  }

}
