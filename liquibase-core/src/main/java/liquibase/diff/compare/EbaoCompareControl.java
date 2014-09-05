package liquibase.diff.compare;

import liquibase.diff.output.EbaoDiffOutputControl;

public class EbaoCompareControl extends CompareControl {

    private EbaoDiffOutputControl diffOutputControl;

    public EbaoCompareControl(SchemaComparison[] schemaComparison, String compareTypes, EbaoDiffOutputControl diffOutputControl) {
        super(schemaComparison, compareTypes);
        this.diffOutputControl = diffOutputControl;
    };
    
    public String getTmpDataDir() {
        return diffOutputControl.getTmpDataDir();
    }

    public boolean isSkipped(String name) {
        return diffOutputControl.isSkipped(name);
    }
    
}
