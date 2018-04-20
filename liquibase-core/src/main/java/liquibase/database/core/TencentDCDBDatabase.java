package liquibase.database.core;

import liquibase.database.DatabaseConnection;
import liquibase.database.ebao.EbaoMysqlDatabase;
import liquibase.exception.DatabaseException;

public class TencentDCDBDatabase extends EbaoMysqlDatabase {

  public static final String PRODUCT_NAME = "DCDB";

  @Override
  public String getShortName() {
    return "dcdb";
  }

  @Override
  public int getPriority() {
    return super.getPriority() + 1;
  }

  @Override
  public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
    return super.isCorrectDatabaseImplementation(conn)
        && PRODUCT_NAME.equalsIgnoreCase(System.getProperty("liquibase.database"));
  }

}
