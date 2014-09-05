package liquibase.changelog.filter;

import liquibase.changelog.ChangeSet;

public class CountDdlChangeSetFilter implements ChangeSetFilter {

  private boolean found = false;

  public CountDdlChangeSetFilter() {
  }

  public ChangeSetFilterResult accepts(ChangeSet changeSet) {
    if (!found && !changeSet.isAlwaysRun() && !changeSet.isRunOnChange()) {
      found = true;
      ChangeSet.setChangeSetMarkedRan(changeSet.getIdentifier());
    }
    return new ChangeSetFilterResult(true, "", this.getClass());
  }
}
