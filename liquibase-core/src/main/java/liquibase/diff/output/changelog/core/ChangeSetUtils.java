package liquibase.diff.output.changelog.core;

import liquibase.changelog.ChangeSet;
import liquibase.database.ObjectQuotingStrategy;

public final class ChangeSetUtils {

    static ChangeSet generateChangeSet(String id) {
        return generateChangeSet(id, false);
    }

    static ChangeSet generateChangeSet(String id, boolean runOnChange) {
      return new ChangeSet(id, "generated", false, runOnChange, null, null, null, ObjectQuotingStrategy.QUOTE_ALL_OBJECTS, null);
    }

}
