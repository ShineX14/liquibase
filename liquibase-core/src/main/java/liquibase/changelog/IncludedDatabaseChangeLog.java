package liquibase.changelog;

import java.util.ArrayList;
import java.util.List;

public class IncludedDatabaseChangeLog extends DatabaseChangeLog {

  private final List<Object> changeSetAndLogs = new ArrayList<Object>();

  public IncludedDatabaseChangeLog(String physicalChangeLogLocation) {
    super(physicalChangeLogLocation);
  }

  @Override
  public void addChangeSet(ChangeSet changeSet) {
    super.addChangeSet(changeSet);
    this.changeSetAndLogs.add(changeSet);
  }

  public List<Object> getChangeSetAndLogs() {
    return this.changeSetAndLogs;
  }

  public void addChangeLog(DatabaseChangeLog changeLog) {
    this.changeSetAndLogs.add(changeLog);
  }

  @Override
  public void setLogicalFilePath(String logicalFilePath) {
    //the calculated path from parent should not be overridden
    //if no logicalFilePath was assigned in the xml contents
    if (logicalFilePath != null) {
      super.setLogicalFilePath(logicalFilePath);
    }
  }
}
