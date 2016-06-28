package liquibase.database.ebao;

import liquibase.database.core.MySQLDatabase;

public class EbaoMysqlDatabase extends MySQLDatabase {

  public EbaoMysqlDatabase() {
    super();
    super.sequenceNextValueFunction = "f_next_seq('%s')";
  }

  @Override
  public int getPriority() {
    return super.getPriority() + 1;
  }
  
}
