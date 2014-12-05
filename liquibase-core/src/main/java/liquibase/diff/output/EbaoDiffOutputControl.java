package liquibase.diff.output;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EbaoDiffOutputControl extends DiffOutputControl {

  private final Map<String, List<TableCondition>> diffTableMap = new LinkedHashMap<String, List<TableCondition>>();

  //generate change log
  private static boolean insertUpdatePreferred = false;

  //compare
  private final List<String> skippedObjects = new ArrayList<String>();

  public EbaoDiffOutputControl(boolean includeCatalog, boolean includeSchema, boolean includeTablespace) {
      super(includeCatalog, includeSchema, includeTablespace);
  }
  
    public void addSkippedObject(String name) {
        skippedObjects.add(name);
    }

    public void addSkippedObjects(List<String> names) {
        skippedObjects.addAll(names);
    }

    public boolean isSkipped(String name) {
        return skippedObjects.contains(name);
    }

  public static boolean isInsertUpdatePreferred() {
    return insertUpdatePreferred;
  }

  public static void setInsertUpdatePreferred(boolean insertUpdatePreferred) {
    EbaoDiffOutputControl.insertUpdatePreferred = insertUpdatePreferred;
  }

  public void addDiffTable(String diffTable, String condition) {
    addDiffTable(diffTable, condition, null, null);
  }

  public void addDiffTable(String diffTable, String condition, String subdir, String filename) {
    diffTable = diffTable.toUpperCase();
    if (condition != null) {
      condition = condition.trim();
      if (!"".equals(condition)
          && !condition.toLowerCase().startsWith("where ")
          && !condition.toLowerCase().startsWith("order by ")
          && !condition.toLowerCase().startsWith("start with")
          && !condition.toLowerCase().startsWith("connect by")
          && !condition.toLowerCase().startsWith("union ")) {
        condition = "where " + condition;
      }
    }

    TableCondition c = new TableCondition();
    c.setName(diffTable);
    c.setCondition(condition);
    c.setSubdir(subdir);
    c.setFilename(filename);

    List<TableCondition> list = diffTableMap.get(diffTable);
    if (list == null) {
      list = new ArrayList<TableCondition>();
      diffTableMap.put(diffTable, list);
    }
    list.add(c);
  }

  public boolean isDiffTable(String tableName) {
	  if (isSkipped(tableName)) {
		return false;
	  }
	  
      if (diffTableMap.isEmpty()) {
          return true;
      }
      return diffTableMap.containsKey(tableName);
  }
  
  public Collection<String> getDiffTables() {
    return diffTableMap.keySet();
  }

  public List<TableCondition> getDiffWhereClause(String tableName) {
    return diffTableMap.get(tableName);
  }

  public class TableCondition {
    private String name;
    private String condition;
    private String subdir;
    private String filename;

    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getCondition() {
      return condition;
    }
    public void setCondition(String condition) {
      this.condition = condition;
    }
    public String getSubdir() {
      return subdir;
    }
    public void setSubdir(String subdir) {
      this.subdir = subdir;
    }
    public String getFilename() {
      return filename;
    }
    public void setFilename(String filename) {
      this.filename = filename;
    }
  }
}
