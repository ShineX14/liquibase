package liquibase.database.ebao;

import liquibase.database.core.OracleDatabase;

public class EbaoOracleDatabase extends OracleDatabase {

  public EbaoOracleDatabase() {
    super();
    super.sequenceNextValueFunction = "f_next_seq('%s')";
  }
  
  @Override
  public int getPriority() {
    return super.getPriority() + 1;
  }
  
}
