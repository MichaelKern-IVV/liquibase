package liquibase.diff.compare.core;

import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparator;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Relation;
import liquibase.structure.core.UniqueConstraint;
import liquibase.util.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class UniqueConstraintComparator implements DatabaseObjectComparator<UniqueConstraint> {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (UniqueConstraint.class.isAssignableFrom(objectType)) {
            return PRIORITY_TYPE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public String[] hash(UniqueConstraint databaseObject, Database accordingTo, DatabaseObjectComparatorChain chain) {
        List<String> hashes = new ArrayList<>();
        if (databaseObject.getName() != null) {
            hashes.add(databaseObject.getName().toLowerCase());
        }

        Relation table = ((UniqueConstraint) databaseObject).getRelation();
        if (table != null) {
            hashes.addAll(Arrays.asList(DatabaseObjectComparatorFactory.getInstance().hash(table, chain.getSchemaComparisons(), accordingTo)));
        }

        return hashes.toArray(new String[0]);
    }

    @Override
    public boolean isSameObject(UniqueConstraint thisConstraint, UniqueConstraint otherConstraint, Database accordingTo, DatabaseObjectComparatorChain chain) {

        int thisConstraintSize = thisConstraint.getColumns().size();
        int otherConstraintSize = otherConstraint.getColumns().size();

        if ((thisConstraint.getRelation() != null) && (otherConstraint.getRelation() != null)) {
            if (!DatabaseObjectComparatorFactory.getInstance().isSameObject(thisConstraint.getRelation(), otherConstraint.getRelation(), chain.getSchemaComparisons(), accordingTo)) {
                return false;
            }
            if ((thisConstraint.getSchema() != null) && (otherConstraint.getSchema() != null) &&
                !DatabaseObjectComparatorFactory.getInstance().isSameObject(thisConstraint.getSchema(),
                    otherConstraint.getSchema(), chain.getSchemaComparisons(), accordingTo)) {
                return false;
            }

            if ((thisConstraint.getName() != null) && (otherConstraint.getName() != null) &&
                DefaultDatabaseObjectComparator.nameMatches(thisConstraint, otherConstraint, accordingTo)) {
                return true;
            } else {
                if ((thisConstraintSize == 0) || (otherConstraintSize == 0)) {
                    return DefaultDatabaseObjectComparator.nameMatches(thisConstraint, otherConstraint, accordingTo);
                }

                if ((thisConstraintSize > 0) && (otherConstraintSize > 0) && (thisConstraintSize != otherConstraintSize)) {
                    return false;
                }

                for (int i = 0; i < otherConstraintSize; i++) {
                    if (!DatabaseObjectComparatorFactory.getInstance().isSameObject(thisConstraint.getColumns().get(i).setRelation(thisConstraint.getRelation()), otherConstraint.getColumns().get(i).setRelation(otherConstraint.getRelation()), chain.getSchemaComparisons(), accordingTo)) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            if ((thisConstraintSize > 0) && (otherConstraintSize > 0) && (thisConstraintSize != otherConstraintSize)) {
                return false;
            }

            if (!DefaultDatabaseObjectComparator.nameMatches(thisConstraint, otherConstraint, accordingTo)) {
                return false;
            }

            if ((thisConstraint.getSchema() != null) && (otherConstraint.getSchema() != null)) {
                return DatabaseObjectComparatorFactory.getInstance().isSameObject(thisConstraint.getSchema(), otherConstraint.getSchema(), chain.getSchemaComparisons(), accordingTo);
            } else {
                return true;
            }
        }
    }

    @Override
    public ObjectDifferences findDifferences(UniqueConstraint databaseObject1, UniqueConstraint databaseObject2, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain chain, Set<String> exclude) {
        exclude.add("name");
        exclude.add("columns");
        exclude.add("backingIndex");
        ObjectDifferences differences = chain.findDifferences(databaseObject1, databaseObject2, accordingTo, compareControl, exclude);

        differences.compare("columns", databaseObject1, databaseObject2, (referenceValue, compareToValue) -> {
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

        differences.compare("backingIndex", databaseObject1, databaseObject2, new ObjectDifferences.StandardCompareFunction(chain.getSchemaComparisons(), accordingTo));
        return differences;
    }
}
