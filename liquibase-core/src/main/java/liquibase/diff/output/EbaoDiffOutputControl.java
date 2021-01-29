package liquibase.diff.output;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import liquibase.database.Database;
import liquibase.structure.core.Table;

public class EbaoDiffOutputControl extends DiffOutputControl {

  private final Map<String, List<TableCondition>> diffTableMap = new LinkedHashMap<String, List<TableCondition>>();

  //generate change log
  private final Database database;
  private boolean insertUpdatePreferred = false;
  private int xmlCsvRowLimit = 1000;
  private int csvRowLimit = 10000;

  //compare
  private final List<String> skippedObjects = new ArrayList<String>();

  public EbaoDiffOutputControl(boolean includeCatalog, boolean includeSchema, boolean includeTablespace, Database database) {
      super(includeCatalog, includeSchema, includeTablespace);
      this.database = database;
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

  public boolean isInsertUpdatePreferred() {
    return insertUpdatePreferred;
  }

  public void setInsertUpdatePreferred(boolean insertUpdatePreferred) {
    this.insertUpdatePreferred = insertUpdatePreferred;
  }

  public int getXmlCsvRowLimit() {
	return xmlCsvRowLimit;
  }

  public void setXmlCsvRowLimit(int xmlCsvRowLimit) {
	this.xmlCsvRowLimit = xmlCsvRowLimit;
  }

  public int getCsvRowLimit() {
    return csvRowLimit;
  }

  public void setCsvRowLimit(int csvRowLimit) {
    this.csvRowLimit = csvRowLimit;
  }

  public void addDiffTable(String diffTable, String condition) {
    addDiffTable(diffTable, condition, null, null);
  }

  public void addDiffTable(String diffTable, String condition, String subdir, String filename) {
    diffTable = database.correctObjectName(diffTable, Table.class);
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
