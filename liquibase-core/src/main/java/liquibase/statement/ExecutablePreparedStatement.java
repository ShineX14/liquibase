package liquibase.statement;

import java.sql.PreparedStatement;
import java.util.List;

import liquibase.change.ColumnConfig;
import liquibase.database.PreparedStatementFactory;
import liquibase.exception.DatabaseException;

/**
 * To be implemented by instances that use a prepared statement for execution
 */
public interface ExecutablePreparedStatement extends SqlStatement {
    /**
     * Execute the prepared statement
     * @param factory for creating a <code>PreparedStatement</code> object
     * @throws DatabaseException
     */
    void execute(PreparedStatementFactory factory) throws DatabaseException;
    void execute(PreparedStatement statement) throws DatabaseException;    

    Info getStatement();
    void setParameter(PreparedStatement stmt) throws DatabaseException;
    
    class Info {
        public final String sql;
        public final List<ColumnConfig> columns;
        public final List<String> parameters;
    
        public Info(String sql, List<ColumnConfig> columns, List<String> parameters) {
          this.sql = sql;
          this.columns = columns;
          this.parameters = parameters;
        }
    }
}
