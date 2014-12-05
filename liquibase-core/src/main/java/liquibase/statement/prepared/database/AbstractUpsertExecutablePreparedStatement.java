package liquibase.statement.prepared.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import liquibase.change.ColumnConfig;
import liquibase.exception.DatabaseException;
import liquibase.statement.prepared.AbstractPreparedStatement;
import liquibase.statement.prepared.InsertExecutablePreparedStatementChange;

public abstract class AbstractUpsertExecutablePreparedStatement extends
		AbstractPreparedStatement {

	protected void setParameter(InsertExecutablePreparedStatementChange change,
			PreparedStatement stmt, List<ColumnConfig> cols)
			throws DatabaseException {
		try {
			setParameter(stmt, 1, cols, change.getChangeSet().getChangeLog()
					.getPhysicalFilePath(), change.getResourceAccessor());
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
