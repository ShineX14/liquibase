package liquibase.changelog.filter;

import liquibase.changelog.ChangeSet;
import liquibase.precondition.Precondition;
import liquibase.precondition.core.PreconditionContainer.ErrorOption;
import liquibase.precondition.core.PreconditionContainer.FailOption;

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
    if (changeSet.getFailOnError() != null) {
      if (!changeSet.getFailOnError()) {
        return false;
      }
    }
//    ErrorOption errorOption = changeSet.getPreconditions().getOnError();
//    FailOption failOption = changeSet.getPreconditions().getOnFail();
    return true;
  }
}
