package liquibase.statement.prepared;

import liquibase.change.ColumnConfig;
import liquibase.change.core.InsertDataChange;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.ExecutablePreparedStatement;
import liquibase.structure.core.Sequence;

public abstract class AbstractLoadPreparedStatement extends AbstractPreparedStatement
    implements ExecutablePreparedStatement {

  protected void copyFromLoadExcutablePreparedStatementChange(InsertDataChange change,
      LoadExecutablePreparedStatementChange orgChange, String[] headers, String[] lines) {
    change.setCatalogName(orgChange.getCatalogName());
    change.setSchemaName(orgChange.getSchemaName());
    change.setDbms(null);
    change.setChangeSet(orgChange.getChangeSet());
    change.setResourceAccessor(orgChange.getResourceAccessor());
    change.setTableName(orgChange.getTableName());
    for (int i=0; i<headers.length; i++) {
      ColumnConfig headerColumn = orgChange.getColumnConfig(i, headers[i]);
      ColumnConfig valueColumn = getColumnConfig(headerColumn, lines[i]);
      change.addColumn(valueColumn);
    }
  }

  private ColumnConfig getColumnConfig(ColumnConfig headerColumn, String value) {
    ColumnConfig valueColumn = new ColumnConfig(); 
    valueColumn.setName(headerColumn.getName());
    if (value != null) {
      if (headerColumn.getType().equalsIgnoreCase("BOOLEAN")) {
        valueColumn.setValueBoolean(Boolean.parseBoolean(value.toString()
            .toLowerCase()));
      } else if (headerColumn.getType().equalsIgnoreCase("NUMERIC")) {
        valueColumn.setValueNumeric(value.toString());
      } else if (headerColumn.getType().toUpperCase().contains("DATE")
          || headerColumn.getType().toUpperCase().contains("TIME")) {
        valueColumn.setValueDate(value.toString());
      } else if (headerColumn.getType().equalsIgnoreCase("STRING")) {
        valueColumn.setValue(value.toString());
      } else if (headerColumn.getType().equalsIgnoreCase("COMPUTED")) {
        if (headerColumn.getDefaultValueComputed() != null) {
          valueColumn.setValueComputed(headerColumn.getDefaultValueComputed());
        } else if (headerColumn.getDefaultValueSequenceNext() != null) {
          valueColumn.setValueComputed(headerColumn.getDefaultValueSequenceNext());
        } else {
          valueColumn.setValueComputed(new DatabaseFunction(value.toString()));
        }
      } else if (headerColumn.getType().equalsIgnoreCase("BLOB")) {
        valueColumn.setValueBlobFile(value.toString());
      } else if (headerColumn.getType().equalsIgnoreCase("CLOB")) {
        valueColumn.setValueClobFile(value.toString());
      } else {
        throw new UnexpectedLiquibaseException("loadData type of " + headerColumn.getType()
            + " is not supported.  Please use BOOLEAN, NUMERIC, DATE, STRING, COMPUTED or SKIP");
      }
    }
    return valueColumn;
  }

}
