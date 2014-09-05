package liquibase.statement.prepared;

import liquibase.change.ChangeWithColumns;
import liquibase.change.ColumnConfig;
import liquibase.change.core.LoadDataColumnConfig;

public interface LoadExecutablePreparedStatementChange extends
    ChangeWithColumns<LoadDataColumnConfig>, ExecutablePreparedStatementChange {

  ColumnConfig getColumnConfig(int index, String header);

}
