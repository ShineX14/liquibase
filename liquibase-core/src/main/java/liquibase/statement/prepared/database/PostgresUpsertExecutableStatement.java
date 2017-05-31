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

public class PostgresUpsertExecutableStatement extends AbstractPreparedStatement {

	private final Database database;
	private final InsertExecutablePreparedStatementChange change;

	private Info statement;
	private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();

	public PostgresUpsertExecutableStatement(Database database,
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

		StringBuilder insertColumnSql = new StringBuilder();
		StringBuilder insertValueSql = new StringBuilder();
		StringBuilder updateSql = new StringBuilder();

		List<String> primaryKeys = getPrimaryKey(change.getPrimaryKey());
		StringBuilder primaryKeyNames = new StringBuilder();
		for (String pk : primaryKeys) {
		  if (primaryKeyNames.length() > 0) {
            primaryKeyNames.append(",");
          }
		  primaryKeyNames.append(pk);
        }
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
					updateSql.append(columnName + "=excluded." + columnName + ",");
				}
			} else {
				cols.add(column);

				insertValueSql.append("?,");
				if (!pkcloum) {
					updateSql.append(columnName + "=excluded." + columnName + ",");
				}
			}
		}

		deleteLastSeperator(insertColumnSql);
		deleteLastSeperator(insertValueSql);
		deleteLastSeperator(updateSql);

		StringBuilder mergeSql = new StringBuilder();
		mergeSql.append("insert into ");
		mergeSql.append(tableName + "(" + insertColumnSql + ")");
		mergeSql.append(" values(" + insertValueSql + ") ");
		if (updateSql.length() > 0) {
		    mergeSql.append(" on conflict(").append(primaryKeyNames).append(") do update set " + updateSql);
		} else {
		  mergeSql.append(" od nothing");
		}
		

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
