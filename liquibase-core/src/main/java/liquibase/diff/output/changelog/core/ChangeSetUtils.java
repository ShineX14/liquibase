package liquibase.diff.output.changelog.core;

import liquibase.changelog.ChangeSet;
import liquibase.database.ObjectQuotingStrategy;

public final class ChangeSetUtils {

    static ChangeSet generateChangeSet(String id) {
        return generateChangeSet(id, null, false);
    }

    static ChangeSet generateChangeSet(String id, String author, boolean runOnChange) {
      if (author == null || author.isEmpty()) {
        author = "generated";
      }
      return new ChangeSet(id, author, false, runOnChange, null, null, null, ObjectQuotingStrategy.QUOTE_ALL_OBJECTS, null);
    }

}
