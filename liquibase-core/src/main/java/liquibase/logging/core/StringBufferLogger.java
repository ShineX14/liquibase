package liquibase.logging.core;

import liquibase.logging.LogLevel;
import liquibase.logging.LoggerSpi;

public class StringBufferLogger extends DefaultLogger {

  private static int LOGGER_BUFFER_MAX = 1000000;
  private static int LOGGER_BUFFER_LIMIT = LOGGER_BUFFER_MAX * 99 / 100;
  private static StringBuffer loggerBuffer = new StringBuffer(LOGGER_BUFFER_MAX);
  private static int deletedBufferSize = 0;
  private static boolean hasSevereLog = false;
  private static LoggerSpi logger;
 
  @Deprecated // kept for compatibility
  public static void enable() {}

  public static void setLogger(LoggerSpi logger) {
    StringBufferLogger.logger = logger;
  }

  static void initTest() {
    LOGGER_BUFFER_MAX = 100;
    LOGGER_BUFFER_LIMIT = 90;
    loggerBuffer = new StringBuffer(LOGGER_BUFFER_MAX);
  }

  public static synchronized void reset() {
    loggerBuffer.setLength(0);
    deletedBufferSize = 0;
    hasSevereLog = false;
  }

  public static boolean hasSevereLog() {
    return hasSevereLog;
  }

  private static final String DOT_LINE = new String(new char[199]).replace('\0', '.') + "\n";

  public static synchronized LogBlock getLogBlock(int start) {
    start = start - deletedBufferSize;
    int length = loggerBuffer.length();
    if (start > length) {
      return new LogBlock("", 0);
    } else if (start >= 0) {
      String log = loggerBuffer.substring(start, length);
      return new LogBlock(log, log.length());
    } else {
      int totalLength = -start + length;
      StringBuilder s = new StringBuilder(DOT_LINE.length() + length);
      s.append(DOT_LINE);
      s.append(loggerBuffer.substring(0, length));
      return new LogBlock(s.toString(), totalLength);
    }
  }
 
  //kept for backward compatibilities
  public static synchronized String getLog(int start) {
    start = start - deletedBufferSize;
    int length = loggerBuffer.length();
    if (start > length) {
      return "";
    } else if (start >= 0) {
      return loggerBuffer.substring(start, length);
    } else {
      int totalLength = -start + length;
      StringBuilder s = new StringBuilder(totalLength);
      for (int i = 0; i < -start / DOT_LINE.length(); i++) {
        s.append(DOT_LINE);
      }
      s.append(DOT_LINE.subSequence(0, -start % DOT_LINE.length()));
      s.append(loggerBuffer.substring(0, length));
      return s.toString();
    }
  }

  @Override
  public int getPriority() {
    return super.getPriority() + 1;
  }

  private static synchronized void printStringBufferLog(String message) {
    loggerBuffer.append(message).append("\n");
    if (loggerBuffer.length() > LOGGER_BUFFER_LIMIT) {
      loggerBuffer.delete(0, LOGGER_BUFFER_MAX / 2);
      deletedBufferSize += LOGGER_BUFFER_MAX / 2;
    }
  }

  @Override
  protected void print(LogLevel logLevel, String message) {
    if (logger == null) {
      super.print(logLevel, message);
    } else {
      if (logLevel == LogLevel.DEBUG) {
        logger.debug(message);
      } else if (logLevel == LogLevel.INFO) {
        logger.info(message);
      } else if (logLevel == LogLevel.WARNING) {
        logger.warn(message);
      } else if (logLevel == LogLevel.SEVERE) {
        logger.error(message);
      } else {
        logger.error(message);
      }
    }
    printStringBufferLog(message);
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
