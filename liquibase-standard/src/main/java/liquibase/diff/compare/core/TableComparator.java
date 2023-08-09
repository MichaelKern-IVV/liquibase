package liquibase.diff.compare.core;

import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparator;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

import java.util.Set;

public class TableComparator implements DatabaseObjectComparator<Table> {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (Table.class.isAssignableFrom(objectType)) {
            return PRIORITY_TYPE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public String[] hash(Table table, Database accordingTo, DatabaseObjectComparatorChain chain) {
        return chain.hash(table, accordingTo);
    }

    @Override
    public boolean isSameObject(Table thisTable, Table otherTable, Database accordingTo, DatabaseObjectComparatorChain chain) {

        //short circut chain.isSameObject for performance reasons. There can be a lot of tables in a database and they are compared a lot
        if (!DefaultDatabaseObjectComparator.nameMatches(thisTable, otherTable, accordingTo)) {
            return false;
        }

        return DatabaseObjectComparatorFactory.getInstance().isSameObject(thisTable.getSchema(), otherTable.getSchema(), chain.getSchemaComparisons(), accordingTo);
    }

    @Override
    public ObjectDifferences findDifferences(Table thisTable, Table otherTable, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain<Table> chain, Set<String> exclude) {
        exclude.add("indexes");
        exclude.add("name");
        exclude.add("outgoingForeignKeys");
        exclude.add("uniqueConstraints");
        exclude.add("primaryKey");
        exclude.add("columns");
        exclude.add("schema");

        ObjectDifferences differences = chain.findDifferences(thisTable, otherTable, accordingTo, compareControl, exclude);
        differences.compare("name", thisTable, otherTable, new ObjectDifferences.DatabaseObjectNameCompareFunction(Table.class, accordingTo));

        if (thisTable.isDefaultTablespace() && otherTable.isDefaultTablespace()) {
            differences.removeDifference("tablespace");
        }
        differences.removeDifference("default_tablespace");
        return differences;
    }
}
