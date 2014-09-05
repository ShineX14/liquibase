package liquibase.diff.output;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DataInterceptor {

  private static List<String> userIdColumnNames;

  public static List<String> getUserIdColumnNames() {
    if (userIdColumnNames == null) {
      userIdColumnNames = new ArrayList<String>();
      try {
        InputStream in = DataInterceptor.class.getResourceAsStream("ls.properties");
        if (in == null) {
          throw new RuntimeException("ls.properties not found in " + DataInterceptor.class.getPackage());
        }
        Properties props = new Properties();
        props.load(in);
        for (Object key : props.keySet()) {
          userIdColumnNames.add((String) key);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return userIdColumnNames;
  }

}
