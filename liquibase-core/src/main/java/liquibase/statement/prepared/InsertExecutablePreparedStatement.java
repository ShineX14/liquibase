package liquibase.statement.prepared;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.statement.ExecutablePreparedStatement;

/**
 * Handles INSERT Execution
 */
public class InsertExecutablePreparedStatement extends
    AbstractPreparedStatement implements ExecutablePreparedStatement {

  private final Database database;
  private final InsertExecutablePreparedStatementChange change;
  private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();

  private Info statement;
  private final int type; //1:insert, 2:update, 3:mysql on duplicate key update,4: insert into() select where not exists

  public InsertExecutablePreparedStatement(Database database,
      InsertExecutablePreparedStatementChange change) {
    this.database = database;
    this.change = change;
    this.type = 1;
  }

  public InsertExecutablePreparedStatement(Database database,
      InsertExecutablePreparedStatementChange change, int type) {
    this.database = database;
    this.change = change;
    this.type = type;
  }

  @Override
  public Info getStatement() {
    if (statement != null) {
      return statement;
    }

    // build the sql statement
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    StringBuilder params = new StringBuilder("VALUES(");
    StringBuilder updateSql = new StringBuilder("UPDATE ");
    
    StringBuilder psql = new StringBuilder();
    StringBuilder pparam = new StringBuilder();
    StringBuilder fields = new StringBuilder();
    StringBuilder fieldsSet = new StringBuilder();
    StringBuilder where1 = new StringBuilder();
    StringBuilder where2 = new StringBuilder();
    
    String tableName = database.escapeTableName(change.getCatalogName(),
        change.getSchemaName(), change.getTableName());
    sql.append(tableName).append("(");
    if (type == 2) {
      updateSql.append(tableName).append(" SET ");
    }

    for (ColumnConfig column : change.getColumns()) {
      if (database.supportsAutoIncrement()
          && Boolean.TRUE.equals(column.isAutoIncrement())) {
        continue;
      }
      String columnName = database.escapeColumnName(change.getCatalogName(),
          change.getSchemaName(), change.getTableName(), column.getName());
      sql.append(columnName).append(", ");
      updateSql.append(columnName).append("=");

      fields.append(columnName).append(", ");
      fieldsSet.append(columnName).append("=nv.").append(columnName).append(", ");
      
      if (column.getValueObject() == null && column.getValueBlobFile() == null
          && column.getValueClobFile() == null) {
        params.append("null, ");
        updateSql.append("null, ");
        pparam.append("null, ");
      } else if (column.getValueComputed() != null && type!=4) {
        params.append(column.getValueComputed().getValue()).append(", ");
        updateSql.append(column.getValueComputed().getValue()).append(", ");
        pparam.append(column.getValueComputed().getValue()).append(", ");
      } else {
        params.append("?, ");
        updateSql.append("?, ");
        if(column.getValueDate()!=null && type == 4){
        	pparam.append("date(?), ");
        }else{
        	pparam.append("?, ");
        }
        cols.add(column);
      }
    }

    sql.deleteCharAt(sql.lastIndexOf(" "));
    sql.deleteCharAt(sql.lastIndexOf(","));
    params.deleteCharAt(params.lastIndexOf(" "));
    params.deleteCharAt(params.lastIndexOf(","));
    updateSql.deleteCharAt(updateSql.lastIndexOf(" "));
    updateSql.deleteCharAt(updateSql.lastIndexOf(","));

    pparam.deleteCharAt(pparam.lastIndexOf(" "));
    pparam.deleteCharAt(pparam.lastIndexOf(","));
    fields.deleteCharAt(fields.lastIndexOf(" "));
    fields.deleteCharAt(fields.lastIndexOf(","));
    fieldsSet.deleteCharAt(fieldsSet.lastIndexOf(" "));
    fieldsSet.deleteCharAt(fieldsSet.lastIndexOf(","));
    
    sql.append(") ");
    params.append(")");
    
    sql.append(params);
    if (type == 2 || type == 4) {
      if (change instanceof UpdateExecutablePreparedStatementChange) {
        String whereClause = ((UpdateExecutablePreparedStatementChange) change)
            .getWhereClause();

        	updateSql.append(" where " + whereClause);
      } else {//insert
    	  if(type==4){
    		  String[] pks = change.getPrimaryKey().split(",");
    		  for(int i=0;i<pks.length;i++){
    			  if(i>0){
    				  where1.append(" and ");
    				  where2.append(" and ");
    			  }
    			  where1.append(" m.").append(pks[i]).append("=nv.").append(pks[i]);
    			  where2.append(" up.").append(pks[i]).append("=new_values.").append(pks[i]);
    		  }
    	  }else{
        updateSql.append(getPrimaryKeyWhereClause(change.getPrimaryKey(),
            change.getColumns(), cols));
    	  }
      }
    }

    String s = sql.toString();
    if (type == 2 || type == 3) {
      s = updateSql.toString();
    }
    if(type == 4){
    	psql.append(" with new_values( ").append(fields).append(" ) as ( ");
    	psql.append(" values( ").append(pparam).append(")), ");
    	psql.append(" upsert as ( update ").append(tableName).append(" m set ");
    	psql.append(fieldsSet).append(" from new_values nv where ").append(where1);
    	psql.append(" returning m.*) ");
    	psql.append(" insert into ").append(tableName).append("(").append(fields).append(")");
    	psql.append(" select ").append(fields).append(" from new_values ");
    	psql.append(" where not exists(select 1 from upsert up where ").append(where2);
    	psql.append(" ) ");
    	s=psql.toString();
    }
    statement = new Info(s, cols, getParameters(cols, change.getChangeSet()
        .getFilePath()));
    return statement;
  }

  @Override
  protected void setParameter(PreparedStatement stmt) throws DatabaseException {
    setParameter(stmt, 1);
  }

  protected void setParameter(PreparedStatement stmt, int paramStartAt)
      throws DatabaseException {
    try {
      setParameter(stmt, paramStartAt, cols, change.getChangeSet().getChangeLog()
          .getPhysicalFilePath(), change.getResourceAccessor());
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  public int getParameterSize() {
    return cols.size();
  }
}
