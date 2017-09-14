package liquibase.logging.core;

import org.junit.Test;

public class StringBufferLoggerTest {

  @Test
  public void test() {
    StringBufferLogger.initTest();
    StringBufferLogger.reset();
    
    StringBufferLogger logger = new StringBufferLogger();
    long logNumber = 100000001;
    
    for (int i = 0; i < 10; i++) {
      logger.info(String.valueOf(logNumber++));
      System.out.println(StringBufferLogger.getLog(0));
      System.out.println(StringBufferLogger.getLog(50));
    }
    
  }
  
}
