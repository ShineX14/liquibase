package com.ebao.tool.liquibase;

import org.liquibase.maven.plugins.LiquibaseUpdate;

/**
 * Applies the DatabaseChangeLogs to the database in a sequential way and mark the next DDL script file ran.
 *
 * @goal markNextChangeSetRan
 */
public class LiquibaseUpdateAndMarkNextChangeSetRan extends LiquibaseUpdate {
	
	@Override
	protected void processSystemProperties() {
		super.processSystemProperties();
		super.markNextChangeSetRan = true;
	}

}