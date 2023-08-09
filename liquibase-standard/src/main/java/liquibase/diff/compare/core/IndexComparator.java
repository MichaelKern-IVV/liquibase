package liquibase.diff.compare.core;

import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparator;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Index;
import liquibase.structure.core.Relation;
import liquibase.util.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class IndexComparator implements DatabaseObjectComparator<Index> {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (Index.class.isAssignableFrom(objectType)) {
            return PRIORITY_TYPE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public String[] hash(Index index, Database accordingTo, DatabaseObjectComparatorChain chain) {
        List<String> hashes = new ArrayList<>();
        if (index.getName() != null) {
            hashes.add(index.getName().toLowerCase());
        }

        Relation table = index.getRelation();
        if (table != null) {
            hashes.addAll(Arrays.asList(DatabaseObjectComparatorFactory.getInstance().hash(table, chain.getSchemaComparisons(), accordingTo)));
        }

        return hashes.toArray(new String[0]);
    }


    @Override
    public boolean isSameObject(Index thisIndex, Index thatIndex, Database accordingTo, DatabaseObjectComparatorChain chain) {

        int thisIndexSize = thisIndex.getColumns().size();
        int thatIndexSize = thatIndex.getColumns().size();

        if ((thisIndex.getRelation() != null) && (thatIndex.getRelation() != null)) {
            if (!DatabaseObjectComparatorFactory.getInstance().isSameObject(thisIndex.getRelation(), thatIndex.getRelation(), chain.getSchemaComparisons(), accordingTo)) {
                return false;
            }
            if ((thisIndex.getSchema() != null) && (thatIndex.getSchema() != null) &&
                !DatabaseObjectComparatorFactory.getInstance().isSameObject(thisIndex.getSchema(),
                		thatIndex.getSchema(), chain.getSchemaComparisons(), accordingTo)) {
                return false;
            }

            if ((thisIndex.getName() != null) && (thatIndex.getName() != null) &&
                DefaultDatabaseObjectComparator.nameMatches(thisIndex, thatIndex, accordingTo)) {
                return true;
            } else {
                if ((thisIndexSize == 0) || (thatIndexSize == 0)) {
                    return DefaultDatabaseObjectComparator.nameMatches(thisIndex, thatIndex, accordingTo);
                }


                if ((thisIndexSize > 0) && (thatIndexSize > 0) && (thisIndexSize != thatIndexSize)) {
                    return false;
                }


                for (int i = 0; i < thatIndexSize; i++) {
                    if (!DatabaseObjectComparatorFactory.getInstance().isSameObject(thisIndex.getColumns().get(i), thatIndex.getColumns().get(i), chain.getSchemaComparisons(), accordingTo)) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            if ((thisIndexSize > 0) && (thatIndexSize > 0) && (thisIndexSize != thatIndexSize)) {
                return false;
            }

            if (!DefaultDatabaseObjectComparator.nameMatches(thisIndex, thatIndex, accordingTo)) {
                return false;
            }

            if ((thisIndex.getSchema() != null) && (thatIndex.getSchema() != null)) {
                return DatabaseObjectComparatorFactory.getInstance().isSameObject(thisIndex.getSchema(), thatIndex.getSchema(), chain.getSchemaComparisons(), accordingTo);
            } else {
                return true;
            }
        }
    }


    @Override
    public ObjectDifferences findDifferences(Index thisIndex, Index thatIndex, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain chain, Set<String> exclude) {
        exclude.add("name");
        exclude.add("columns");
        ObjectDifferences differences = chain.findDifferences(thisIndex, thatIndex, accordingTo, compareControl, exclude);

        differences.compare("columns", thisIndex, thatIndex, (referenceValue, compareToValue) -> {
            List<Column> referenceList = (List) referenceValue;
            List<Column> compareList = (List) compareToValue;

            if (referenceList.size() != compareList.size()) {
                return false;
            }
            for (int i=0; i<referenceList.size(); i++) {
                //
                // Check for nulls
                // If both reference and comparison objects are null then return true
                // else if only one is null then return false
                //
                if (referenceList.get(i) == null || compareList.get(i) == null) {
                    return referenceList.get(i) == compareList.get(i);
                }
                if (!StringUtil.trimToEmpty((referenceList.get(i)).getName()).equalsIgnoreCase(StringUtil.trimToEmpty(compareList.get(i).getName()))) {
                    return false;
                }
            }
            return true;
        });

        return differences;
    }
}
