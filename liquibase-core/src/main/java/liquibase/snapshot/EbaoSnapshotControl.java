package liquibase.snapshot;

import liquibase.database.Database;
import liquibase.diff.output.EbaoDiffOutputControl;
import liquibase.structure.DatabaseObject;

public class EbaoSnapshotControl extends SnapshotControl {

  private EbaoDiffOutputControl diffOutputControl;

  public EbaoSnapshotControl(Database database, String types,
      EbaoDiffOutputControl diffOutputControl) {
    super(database, types);
    this.diffOutputControl = diffOutputControl;
  }

  public EbaoSnapshotControl(Database database, Class<? extends DatabaseObject> type,
      EbaoDiffOutputControl diffOutputControl) {
    super(database, type);
    this.diffOutputControl = diffOutputControl;
  }

  public EbaoSnapshotControl(Database database, EbaoDiffOutputControl diffOutputControl,
      Class<? extends DatabaseObject>... types) {
    super(database, types);
    this.diffOutputControl = diffOutputControl;
  }

  public boolean isIncluded(String name) {
    return diffOutputControl.isDiffTable(name);
  }

}
