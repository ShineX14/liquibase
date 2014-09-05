package com.ebao.tool.liquibase;

import org.liquibase.maven.plugins.LiquibaseUpdateSQL;

/**
 * Generates the SQL that is required to update the database to the current
 * version as specified in the DatabaseChangeLogs.
 * 
 * @author Peter Murray
 * @description Liquibase UpdateSQL Maven plugin
 * @goal updateSQL2
 */
@Deprecated //kept for backward compatibility
public class LiquibaseUpdateSQL2 extends LiquibaseUpdateSQL {

}