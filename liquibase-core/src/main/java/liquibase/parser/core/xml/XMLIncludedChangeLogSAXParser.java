package liquibase.parser.core.xml;

import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.IncludedDatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.resource.ResourceAccessor;

public class XMLIncludedChangeLogSAXParser extends XMLChangeLogSAXParser {

    private static int priority = PRIORITY_DEFAULT - 1; 
    
    public static void setHighPriority() {
        priority = PRIORITY_DATABASE + 1;
      }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public DatabaseChangeLog parse(String physicalChangeLogLocation, ChangeLogParameters changeLogParameters, ResourceAccessor resourceAccessor)
            throws ChangeLogParseException {
        DatabaseChangeLog changeLog = new IncludedDatabaseChangeLog(physicalChangeLogLocation);
        return parse(changeLog, changeLogParameters, resourceAccessor);
    }

}
