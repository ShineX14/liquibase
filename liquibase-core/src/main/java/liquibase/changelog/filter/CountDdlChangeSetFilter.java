package liquibase.changelog.filter;

import liquibase.changelog.ChangeSet;

public class CountDdlChangeSetFilter implements ChangeSetFilter {

  private boolean found = false;

  public CountDdlChangeSetFilter() {}

  public ChangeSetFilterResult accepts(ChangeSet changeSet) {
    if (!found && match(changeSet)) {
      found = true;
      ChangeSet.setChangeSetMarkedRan(changeSet.getIdentifier());
    }
    return new ChangeSetFilterResult(true, "", this.getClass());
  }

  private boolean match(ChangeSet changeSet) {
    if (changeSet.isAlwaysRun()) {
      return false;
    }
    if (changeSet.getFailOnError() != null && changeSet.getFailOnError()) {
      return false;
    }
    return true;
  }
}
