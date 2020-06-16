package liquibase.logging.core;
 
public class LogBlock {
 
  private final String log;
  private final int size;
 
  public LogBlock(String log, int size) {
    this.log = log;
    this.size = size;
  }
 
  public String getLog() {
    return log;
  }
  public int getSize() {
    return size;
  }
 
}