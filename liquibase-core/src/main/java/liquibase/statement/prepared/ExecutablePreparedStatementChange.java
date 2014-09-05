package liquibase.statement.prepared;

import liquibase.changelog.ChangeSet;
import liquibase.resource.ResourceAccessor;

public interface ExecutablePreparedStatementChange {

  ChangeSet getChangeSet();

  ResourceAccessor getResourceAccessor();

  String getCatalogName();

  String getSchemaName();

  String getTableName();

  String getPrimaryKey();

}
