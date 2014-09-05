package liquibase.diff.output.changelog.core;

import liquibase.changelog.ChangeSet;
import liquibase.database.ObjectQuotingStrategy;

public final class ChangeSetUtils {

    static ChangeSet generateChangeSet(String id) {
        return new ChangeSet(id, "generated", false, false, null, null, null, ObjectQuotingStrategy.QUOTE_ALL_OBJECTS, null);
    }

}
