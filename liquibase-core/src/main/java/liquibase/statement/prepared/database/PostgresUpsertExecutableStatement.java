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

public class PostgresUpsertExecutableStatement extends
		AbstractPreparedStatement {

	private final Database database;
	private final InsertExecutablePreparedStatementChange change;

	private Info statement;
	private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();
	private final List<ColumnConfig> pkcols = new ArrayList<ColumnConfig>();

	public PostgresUpsertExecutableStatement(Database database,
			InsertExecutablePreparedStatementChange change) {
		this.database = database;
		this.change = change;
	}

	@Override
	public Info getStatement() {
		if (statement != null) {
			return statement;
		}

		StringBuilder params = new StringBuilder();
		StringBuilder fields = new StringBuilder();
		StringBuilder updateFields = new StringBuilder();
		StringBuilder whereClause = new StringBuilder();

		String tableName = database.escapeTableName(change.getCatalogName(),
				change.getSchemaName(), change.getTableName());

        List<String> primaryKeys = getPrimaryKey(change.getPrimaryKey());
		for (ColumnConfig column : change.getColumns()) {
			if (database.supportsAutoIncrement()
					&& Boolean.TRUE.equals(column.isAutoIncrement())) {
				continue;
			}
			String columnName = database.escapeColumnName(
					change.getCatalogName(), change.getSchemaName(),
					change.getTableName(), column.getName());

            boolean isPkColumn = primaryKeys.contains(columnName);
			fields.append(columnName).append(",");
			if (!isPkColumn) {
			  updateFields.append(columnName + "=nv." + columnName + ",");
            }

            Object valueObject = column.getValueObject();
            if (isPkColumn) {
              if (whereClause.length() > 0) {
                whereClause.append(" and ");
              }
              whereClause.append("t.").append(columnName).append("=?");
              pkcols.add(column);
            }
            if (valueObject == null) {
				params.append("null,");
            } else if (valueObject instanceof DatabaseFunction) {
              String value = database.generateDatabaseFunctionValue((DatabaseFunction)valueObject);
              params.append(value);
			} else {
				if (column.getValueDate() != null) {
					params.append("date(?),");
				} else {
					params.append("?,");
				}
				cols.add(column);
			}
		}

		deleteLastSeperator(params);
		deleteLastSeperator(fields);
		deleteLastSeperator(updateFields);

		StringBuilder sql = new StringBuilder();
		sql.append("with new_values( " + fields + " ) as (");
		sql.append("values( " + params + ")), ");
		sql.append("upsert as ( update " + tableName + " t ");
		sql.append("set " + updateFields + " from new_values nv where " + whereClause);
		sql.append(" returning t.*) ");
		sql.append("insert into " + tableName + "(" + fields + ") ");
		sql.append("select " + fields + " from new_values ");
		sql.append("where not exists (select 1 from " + tableName + " t where " + whereClause + ")");

		String s = sql.toString();
		statement = new Info(s, cols, getParameters(cols, change.getChangeSet()
				.getFilePath()));
		return statement;
	}

	@Override
	public void setParameter(PreparedStatement stmt) throws DatabaseException {
		try {
			setParameter(stmt, 1, cols, change.getChangeSet().getChangeLog()
					.getPhysicalFilePath(), change.getResourceAccessor());
			setParameter(stmt, cols.size() + 1, pkcols, change.getChangeSet().getChangeLog()
			    .getPhysicalFilePath(), change.getResourceAccessor());
			setParameter(stmt, cols.size() + pkcols.size() + 1, pkcols, change.getChangeSet().getChangeLog()
			    .getPhysicalFilePath(), change.getResourceAccessor());
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}
}
