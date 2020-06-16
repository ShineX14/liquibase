package liquibase.logging;
 
public interface LoggerSpi {
 
  void debug(String message);
  void info(String message);
  void warn(String message);
  void error(String message);
 
}