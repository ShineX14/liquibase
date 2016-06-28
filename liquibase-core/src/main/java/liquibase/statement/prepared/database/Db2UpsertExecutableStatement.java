package liquibase.statement.prepared.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.prepared.AbstractPreparedStatement;
import liquibase.statement.prepared.InsertExecutablePreparedStatementChange;

public class Db2UpsertExecutableStatement extends AbstractPreparedStatement {

	private final Database database;
	private final InsertExecutablePreparedStatementChange change;

	private Info statement;
	private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();

	public Db2UpsertExecutableStatement(Database database,
			InsertExecutablePreparedStatementChange change) {
		this.database = database;
		this.change = change;
	}

	public Info getStatement() {
		if (statement != null) {
			return statement;
		}

		String tableName = database.escapeTableName(change.getCatalogName(),
				change.getSchemaName(), change.getTableName());

		StringBuilder columnSql = new StringBuilder();
		StringBuilder columnValueSql = new StringBuilder();
		StringBuilder insertColumnSql = new StringBuilder();
		StringBuilder insertValueSql = new StringBuilder();
		StringBuilder updateSql = new StringBuilder();

		List<String> primaryKeys = getPrimaryKey(change.getPrimaryKey());
		for (ColumnConfig column : change.getColumns()) {
			if (database.supportsAutoIncrement()
					&& Boolean.TRUE.equals(column.isAutoIncrement())) {
				continue;
			}

			String columnName = database.escapeColumnName(
					change.getCatalogName(), change.getSchemaName(),
					change.getTableName(), column.getName());

			insertColumnSql.append(columnName).append(",");

			boolean pkcloum = primaryKeys.contains(columnName);
            Object valueObject = column.getValueObject();
            if (valueObject == null) {
				insertValueSql.append("null,");
				if (!pkcloum) {
					updateSql.append(columnName + "=null,");
				}
            } else if (valueObject instanceof DatabaseFunction) {
              String function = database.generateDatabaseFunctionValue((DatabaseFunction)valueObject);
				insertValueSql.append(function + ",");
				if (!pkcloum) {
					updateSql.append(columnName + "=" + function + ",");
				}
			} else {
				columnSql.append(columnName).append(",");
				columnValueSql.append("?,");
				cols.add(column);

				insertValueSql.append("s.").append(columnName).append(",");
				if (!pkcloum) {
					updateSql.append(columnName).append("=s.")
							.append(columnName).append(",");
				}
			}
		}

		deleteLastSeperator(columnSql);
		deleteLastSeperator(columnValueSql);
		deleteLastSeperator(insertColumnSql);
		deleteLastSeperator(insertValueSql);
		deleteLastSeperator(updateSql);

		StringBuilder mergeSql = new StringBuilder();
		mergeSql.append("merge " + tableName + " t ");
		mergeSql.append("using (table(values(").append(columnValueSql)
				.append("))) as s(").append(columnSql).append(")");
		String onClause = getPrimaryKeyClause(change.getPrimaryKey(), "s", "t");
		mergeSql.append(" on " + onClause);
		if (updateSql.length() > 0) {
		    mergeSql.append(" when matched then update set ").append(updateSql);
		}
		mergeSql.append(" when not matched then");
		mergeSql.append(" insert(").append(insertColumnSql).append(")");
		mergeSql.append(" values(").append(insertValueSql).append(");");

		String s = mergeSql.toString();
		statement = new Info(s, cols, getParameters(cols, change.getChangeSet()
				.getFilePath()));
		return statement;
	}

	@Override
	public void setParameter(PreparedStatement stmt) throws DatabaseException {
		try {
			setParameter(stmt, 1, cols, change.getChangeSet().getChangeLog()
					.getPhysicalFilePath(), change.getResourceAccessor());
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}
}
