package liquibase.exception;

public class BatchUpdateDatabaseException extends DatabaseException {

  private static final long serialVersionUID = 1L;
  
  public BatchUpdateDatabaseException(Throwable cause) {
    super("error in batch update.", cause);
  }

}
