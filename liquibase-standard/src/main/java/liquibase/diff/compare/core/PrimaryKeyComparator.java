package liquibase.diff.compare.core;

import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparator;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.PrimaryKey;
import liquibase.util.StringUtil;

import java.util.List;
import java.util.Set;

public class PrimaryKeyComparator implements DatabaseObjectComparator<PrimaryKey> {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (PrimaryKey.class.isAssignableFrom(objectType)) {
            return PRIORITY_TYPE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public String[] hash(PrimaryKey primaryKey, Database accordingTo, DatabaseObjectComparatorChain chain) {

        if (primaryKey.getName() == null) {
            return DatabaseObjectComparatorFactory.getInstance().hash(primaryKey.getTable(),chain.getSchemaComparisons(), accordingTo);
        } else {
            if ((primaryKey.getTable() == null) || (primaryKey.getTable().getName() == null)) {
                return new String[] {primaryKey.getName().toLowerCase() };
            } else {
                return new String[] {primaryKey.getName().toLowerCase(), primaryKey.getTable().getName().toLowerCase()};
            }
        }
    }

    @Override
    public boolean isSameObject(PrimaryKey thisPrimaryKey, PrimaryKey otherPrimaryKey, Database accordingTo, DatabaseObjectComparatorChain chain) {

        if ((thisPrimaryKey.getTable() != null) && (thisPrimaryKey.getTable().getName() != null) && (otherPrimaryKey
            .getTable() != null) && (otherPrimaryKey.getTable().getName() != null)) {
            return DatabaseObjectComparatorFactory.getInstance().isSameObject(thisPrimaryKey.getTable(), otherPrimaryKey.getTable(), chain.getSchemaComparisons(), accordingTo);
        } else {
            return StringUtil.trimToEmpty(thisPrimaryKey.getName()).equalsIgnoreCase(otherPrimaryKey.getName());
        }
    }

    @Override
    public ObjectDifferences findDifferences(PrimaryKey thisPrimaryKey, PrimaryKey otherPrimaryKey, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain<PrimaryKey> chain, Set<String> exclude) {
        exclude.add("name");
        exclude.add("backingIndex");
        exclude.add("columns");
        ObjectDifferences differences = chain.findDifferences(thisPrimaryKey, otherPrimaryKey, accordingTo, compareControl, exclude);

        differences.compare("columns", thisPrimaryKey, otherPrimaryKey, (referenceValue, compareToValue) -> {
            List<Column> referenceList = (List) referenceValue;
            List<Column> compareList = (List) compareToValue;

            if (referenceList.size() != compareList.size()) {
                return false;
            }
            for (int i=0; i<referenceList.size(); i++) {
                if (!StringUtil.trimToEmpty((referenceList.get(i)).getName()).equalsIgnoreCase(StringUtil.trimToEmpty(compareList.get(i).getName()))) {
                    return false;
                }
            }
            return true;
        });

        return differences;
    }
}
