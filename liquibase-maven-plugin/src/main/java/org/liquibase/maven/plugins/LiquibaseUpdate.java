package org.liquibase.maven.plugins;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.changelog.ChangeSet;
import liquibase.exception.LiquibaseException;
import liquibase.parser.core.xml.XMLIncludedChangeLogSAXParser;
import liquibase.Liquibase;

/**
 * Applies the DatabaseChangeLogs to the database. Useful as part of the build
 * process.
 * 
 * @author Peter Murray
 * @description Liquibase Update Maven plugin
 * @goal update
 */
public class LiquibaseUpdate extends AbstractLiquibaseUpdateMojo {

    /**
     * Whether or not to perform a drop on the database before executing the change.
     * @parameter expression="${liquibase.dropFirst}" default-value="false"
     */
    protected boolean dropFirst;

    /**
     * Whether or not to load included files lazily and perform SQL with batch PreparedStatement.
     * @parameter expression="${liquibase.batchMode}" default-value="true"
     */
    protected boolean batchMode;

    /**
     * Mark changeSet ran if it's fixed and ran manually.
     * @parameter expression="${liquibase.markChangeSetRan}"
     */
    protected String markChangeSetRan;

    /**
     * Mark next DDl changeSet ran if it's fixed and ran manually.
     * @parameter expression="${liquibase.markNextDdlRan}" default-value="false"
     */
    protected boolean markNextChangeSetRan;
    
    /**
     * Whether or not to skip DDL SQL file.
     * @parameter expression="${liquibase.skipDdlSqlFile}" default-value="false"
     */
    protected boolean skipDdlSqlFile;

    @Override
    protected void doUpdate(Liquibase liquibase) throws LiquibaseException {
        if (dropFirst) {
            liquibase.dropAll();
        }
        if (skipDdlSqlFile) {
            ChangeSet.setSkipDdlSqlFile();
        }
        if (markChangeSetRan != null) {
            ChangeSet.setChangeSetMarkedRan(markChangeSetRan);
        }
        if (batchMode) {
            XMLIncludedChangeLogSAXParser.setHighPriority();
            Liquibase.setBatchUpdate();
            Liquibase.setPreparedStatementPreferred();
        }

        if (changesToApply > 0) {
            liquibase.update(changesToApply, new Contexts(contexts), new LabelExpression(labels));
        } else {
            if (markNextChangeSetRan) {
                Liquibase.setMarkNextChangeSetRan();
            }
            liquibase.update(new Contexts(contexts), new LabelExpression(labels));
        }
    }

    @Override
    protected void printSettings(String indent) {
        super.printSettings(indent);
        if (dropFirst) {
            getLog().info(indent + "drop first? " + dropFirst);
        }
        if (batchMode) {
            getLog().info(indent + "batch mode? " + batchMode);
        }
        if (skipDdlSqlFile) {
            getLog().info(indent + "skip DDL SQL file? " + skipDdlSqlFile);
        }
        if (markChangeSetRan != null) {
            getLog().info(indent + "markChangeSetRan: " + markChangeSetRan);
        }
        if (markNextChangeSetRan) {
            getLog().info(indent + "markNextDdlRan: " + markNextChangeSetRan);
        }

    }
}