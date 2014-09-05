package liquibase.parser.core.xml;

import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.core.ParsedNode;
import liquibase.resource.ResourceAccessor;

public abstract class AbstractChangeLogParser implements ChangeLogParser {

    private static Logger log = LogFactory.getInstance().getLog();
    
    @Override
    public DatabaseChangeLog parse(String physicalChangeLogLocation, ChangeLogParameters changeLogParameters, ResourceAccessor resourceAccessor) throws ChangeLogParseException {
        DatabaseChangeLog changeLog = new DatabaseChangeLog();
        changeLog.setPhysicalFilePath(physicalChangeLogLocation);
        return parse(changeLog, changeLogParameters, resourceAccessor);
    }

    @Override
    public DatabaseChangeLog parse(DatabaseChangeLog changeLog, ChangeLogParameters changeLogParameters, ResourceAccessor resourceAccessor) throws ChangeLogParseException {
        String physicalChangeLogLocation = changeLog.getPhysicalFilePath();
        log.info(physicalChangeLogLocation);
        
        changeLog.setChangeLogParameters(changeLogParameters);

        ParsedNode parsedNode = parseToNode(changeLog, changeLogParameters, resourceAccessor);
        if (parsedNode == null) {
            return null;
        }

        try {
            changeLog.load(parsedNode, resourceAccessor);
        } catch (Exception e) {
            throw new ChangeLogParseException(e);
        }

        return changeLog;
    }

    protected abstract ParsedNode parseToNode(DatabaseChangeLog changeLog, ChangeLogParameters changeLogParameters, ResourceAccessor resourceAccessor) throws ChangeLogParseException;
}
