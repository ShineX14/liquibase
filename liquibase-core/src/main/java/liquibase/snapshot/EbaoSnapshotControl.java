package liquibase.snapshot;

import liquibase.database.Database;
import liquibase.diff.output.EbaoDiffOutputControl;

public class EbaoSnapshotControl extends SnapshotControl {

    private EbaoDiffOutputControl diffOutputControl;
    
    public EbaoSnapshotControl(Database database, String types, EbaoDiffOutputControl diffOutputControl) {
        super(database, types);
        this.diffOutputControl = diffOutputControl;
    }

    public boolean isIncluded(String name) {
        return diffOutputControl.isDiffTable(name);
    }

}
