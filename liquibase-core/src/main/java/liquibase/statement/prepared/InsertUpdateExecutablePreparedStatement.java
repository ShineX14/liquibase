package liquibase.statement.prepared;

import java.sql.PreparedStatement;

import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.DatabaseException;
import liquibase.statement.ExecutablePreparedStatement;
import liquibase.statement.prepared.database.MssqlUpsertExecutableStatement;
import liquibase.statement.prepared.database.MysqlUpsertExecutablePreparedStatement;
import liquibase.statement.prepared.database.OracleUpsertExecutablePreparedStatement;
import liquibase.statement.prepared.database.PostgresUpsertExecutableStatement;

public class InsertUpdateExecutablePreparedStatement extends
		AbstractPreparedStatement implements ExecutablePreparedStatement {

	private final AbstractPreparedStatement upsert;

	public InsertUpdateExecutablePreparedStatement(Database database,
			InsertExecutablePreparedStatementChange change) {
		if (database instanceof OracleDatabase) {
			upsert = new OracleUpsertExecutablePreparedStatement(database, change);
		} else if (database instanceof MySQLDatabase) {
			upsert = new MysqlUpsertExecutablePreparedStatement(database, change);
		} else if (database instanceof PostgresDatabase) {
			upsert = new PostgresUpsertExecutableStatement(database, change);
		} else if (database instanceof MSSQLDatabase) {
			upsert = new MssqlUpsertExecutableStatement(database, change);
		} else {
			upsert = new MssqlUpsertExecutableStatement(database, change);
		}
	}

	@Override
	public Info getStatement() {
		return upsert.getStatement();
	}

	public void setParameter(PreparedStatement stmt) throws DatabaseException {
		upsert.setParameter(stmt);
	}

}
