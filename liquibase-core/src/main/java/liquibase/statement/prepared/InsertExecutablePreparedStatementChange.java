package liquibase.statement.prepared;

import liquibase.change.ChangeWithColumns;
import liquibase.change.ColumnConfig;

public interface InsertExecutablePreparedStatementChange extends
    ChangeWithColumns<ColumnConfig>, ExecutablePreparedStatementChange {

}
