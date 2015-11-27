package liquibase.diff.compare;

import java.util.Set;

import liquibase.diff.output.EbaoDiffOutputControl;
import liquibase.structure.DatabaseObject;

public class EbaoCompareControl extends CompareControl {

    private EbaoDiffOutputControl diffOutputControl;

    public EbaoCompareControl(SchemaComparison[] schemaComparison, String compareTypes, EbaoDiffOutputControl diffOutputControl) {
        super(schemaComparison, compareTypes);
        this.diffOutputControl = diffOutputControl;
    };

    public boolean isSkipped(String name) {
        return diffOutputControl != null ? diffOutputControl.isSkipped(name) : false;
    }
    
    public EbaoCompareControl(Set<Class<? extends DatabaseObject>> compareTypes) {
    	super(compareTypes);
    }
    
}
