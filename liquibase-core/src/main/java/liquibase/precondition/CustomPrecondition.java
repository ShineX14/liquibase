package liquibase.precondition;

import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.CustomPreconditionErrorException;
import liquibase.exception.CustomPreconditionFailedException;

public interface CustomPrecondition {
    void check(Database database, DatabaseChangeLog changeLog) throws CustomPreconditionFailedException, CustomPreconditionErrorException;
}
