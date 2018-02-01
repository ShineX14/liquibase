package liquibase.diff.output;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataInterceptor {

  private static Map<String, Long> userIdColumns;
  private static Map<String, Map<String, Long>> userIdTableColumns;

  public static void initUserColumnProperty(InputStream in) throws IOException {
    if (userIdColumns == null) {
      userIdColumns = new HashMap<String, Long>();
      userIdTableColumns = new HashMap<String, Map<String, Long>>();

      Properties props = new Properties();
      props.load(in);
      for (Object key : props.keySet()) {
        String name = (String) key;
        Long value = null;
        if (props.getProperty(name) != null && !props.getProperty(name).isEmpty()) {
          value = Long.parseLong(props.getProperty(name));
        }
        
        String[] names = name.toUpperCase().split("\\.");
        if (names.length == 1) {
          userIdColumns.put(name.toUpperCase(), value);
        } else if (names.length == 2) {
          Map<String, Long> map = userIdTableColumns.get(names[0]);
          if (map == null) {
            map = new HashMap<String, Long>();
            userIdTableColumns.put(names[0], map);
          }
          Long put = map.put(names[1], value);
          if (put != null) {
            throw new IllegalArgumentException(
                "Duplicated column found: " + names[0] + "." + names[1]);
          }
        } else {
          throw new IllegalArgumentException(name);
        }
      }
    }
  }

  public static Long getUserIdColumnValue(String tableName, String columnName) {
    tableName = tableName.toUpperCase();
    columnName = columnName.toUpperCase();
    Map<String, Long> map = userIdTableColumns.get(tableName);
    if (map != null) {
      if (map.containsKey(columnName)) {
        return map.get(columnName);
      }
    }
    return userIdColumns.get(columnName);
  }

}
