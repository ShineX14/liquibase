package liquibase.logging.core;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.LogLevel;
import liquibase.logging.Logger;

public abstract class AbstractLogger implements Logger {
    private LogLevel logLevel;
    private DatabaseChangeLog databaseChangeLog;
    private ChangeSet changeSet;

    @Override
    public LogLevel getLogLevel() {
        return logLevel;
    }

    @Override
    public void setLogLevel(String logLevel) {
        setLogLevel(toLogLevel(logLevel));
    }

    protected LogLevel toLogLevel(String logLevel) {
        if ("debug".equalsIgnoreCase(logLevel)) {
            return LogLevel.DEBUG;
        } else if ("info".equalsIgnoreCase(logLevel)) {
            return LogLevel.INFO;
        } else if ("warning".equalsIgnoreCase(logLevel)) {
            return LogLevel.WARNING;
        } else if ("severe".equalsIgnoreCase(logLevel)) {
            return LogLevel.SEVERE;
        } else if ("off".equalsIgnoreCase(logLevel)) {
            return LogLevel.OFF;
        } else {
            throw new UnexpectedLiquibaseException("Unknown log level: " + logLevel+".  Valid levels are: debug, info, warning, severe, off");
        }
    }

    protected String buildMessage(String message) {
        StringBuilder msg = new StringBuilder();
        if(databaseChangeLog != null) {
            String name = databaseChangeLog.getPhysicalFilePath();
            if (message == null || !message.contains(name)) {
            	msg.append(name).append(": ");
			}
        }
        if(changeSet != null) {
            String name = changeSet.toString(false);
            if (message == null || !message.contains(name)) {
            	msg.append(name.replace(name + "::", "")).append(": ");
			}
        }
        msg.append(message);
        return msg.toString();
    }

    @Override
    public void setLogLevel(LogLevel level) {
        this.logLevel = level;
    }

    @Override
    public void setChangeLog(DatabaseChangeLog databaseChangeLog) {
        this.databaseChangeLog = databaseChangeLog;
    }

    @Override
    public void setChangeSet(ChangeSet changeSet) {
        this.changeSet = changeSet;
    }
}
