package liquibase.changelog;

import java.util.ArrayList;
import java.util.List;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.RuntimeEnvironment;
import liquibase.changelog.filter.ChangeSetFilter;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.exception.LiquibaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;

public class IncludedChangeLogIterator extends ChangeLogIterator {

    private final IncludedDatabaseChangeLog databaseChangeLog;
    private final Liquibase liquibase;
    private final ChangeSetFilter[] changeSetFilters;

    public IncludedChangeLogIterator(Liquibase liquibase, Contexts contexts, IncludedDatabaseChangeLog databaseChangeLog,
            ChangeSetFilter... changeSetFilters) {
        super(databaseChangeLog, changeSetFilters);
        this.liquibase = liquibase;
        this.databaseChangeLog = databaseChangeLog;
        this.changeSetFilters = changeSetFilters;
    }

    @Override
    public void run(ChangeSetVisitor visitor, RuntimeEnvironment env) throws LiquibaseException {
        Logger log = LogFactory.getLogger();
        databaseChangeLog.setRuntimeEnvironment(env);
        log.setChangeLog(databaseChangeLog);
        try {
            List<Object> changeSetOrLogList = new ArrayList<Object>(databaseChangeLog.getChangeSetAndLogs());
            if (visitor.getDirection().equals(ChangeSetVisitor.Direction.REVERSE)) {
                throw new UnsupportedOperationException(ChangeSetVisitor.Direction.REVERSE.toString());
            }

            for (Object changeSetOrLog : changeSetOrLogList) {
                if (changeSetOrLog instanceof IncludedDatabaseChangeLog) {
                    run(visitor, env, (IncludedDatabaseChangeLog) changeSetOrLog);
                } else if (changeSetOrLog instanceof ChangeSet) {
                    log.setChangeLog(databaseChangeLog);// reset
                    super.run(visitor, env, (ChangeSet) changeSetOrLog);
                } else {
                    throw new IllegalArgumentException(changeSetOrLog.getClass().getName());
                }
            }
        } finally {
            log.setChangeLog(null);
            databaseChangeLog.setRuntimeEnvironment(null);
        }
    }

    private void run(ChangeSetVisitor visitor, RuntimeEnvironment env, IncludedDatabaseChangeLog includedChangeLog) throws LiquibaseException {
        String changeLogFile = includedChangeLog.getPhysicalFilePath();

        // new instance to avoid OutOfMemory issue in case of high volume data
        DatabaseChangeLog runnableChangeLog = liquibase.getDatabaseChangeLog(changeLogFile);
        runnableChangeLog.setLogicalFilePath(includedChangeLog.getLogicalFilePath());
        
        liquibase._update(runnableChangeLog, env.getContexts(), env.getLabels(), changeSetFilters);
    }

}
