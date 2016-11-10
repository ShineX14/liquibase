package liquibase.exception;

public class BatchUpdateDatabaseException extends DatabaseException {

  private static final long serialVersionUID = 1L;
  private int statementSize;
  
  public BatchUpdateDatabaseException(int statementSize, Throwable cause) {
    super("error in batch update.", cause);
    this.statementSize = statementSize;
  }
  
  public int getStatementSize() {
    return statementSize;
  }
}
