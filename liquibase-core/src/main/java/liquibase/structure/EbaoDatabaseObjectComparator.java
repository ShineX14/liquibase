package liquibase.structure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import liquibase.changelog.IncludedFile;
import liquibase.structure.core.Data;

public class EbaoDatabaseObjectComparator extends DatabaseObjectComparator {

    private final List<String> tables = new ArrayList<String>();
    
    public EbaoDatabaseObjectComparator(Collection<String> tables) {
        this.tables.addAll(tables);
    }
    
    @Override
    public int compare(DatabaseObject o1, DatabaseObject o2) {
        if (!tables.isEmpty() && o1 instanceof Data && o2 instanceof Data) {
            Data if1 = (Data) o1;
            Data if2 = (Data) o2;
            int index1 = tables.indexOf(if1.getName());
            int index2 = tables.indexOf(if2.getName());
            if (index1 != index2) {
                return index1 - index2;
            }
        }
        return super.compare(o1, o2);
    }
}
