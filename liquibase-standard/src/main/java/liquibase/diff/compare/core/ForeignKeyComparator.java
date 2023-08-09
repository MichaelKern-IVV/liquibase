package liquibase.diff.compare.core;

import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparator;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Table;
import liquibase.util.StringUtil;

import java.util.Set;

public class ForeignKeyComparator implements DatabaseObjectComparator<ForeignKey> {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (ForeignKey.class.isAssignableFrom(objectType)) {
            return PRIORITY_TYPE;
        }
        return PRIORITY_NONE;
    }


    @Override
    public String[] hash(ForeignKey foreignKey, Database accordingTo, DatabaseObjectComparatorChain chain) {

        if (foreignKey.getName() == null) {
            return DatabaseObjectComparatorFactory.getInstance().hash(foreignKey.getForeignKeyTable(), chain.getSchemaComparisons(), accordingTo);
        } else {
            if ((foreignKey.getForeignKeyTable() == null) || (foreignKey.getForeignKeyTable().getName() == null)) {
                return new String[]{foreignKey.getName().toLowerCase()};
            } else {
                return new String[]{foreignKey.getName().toLowerCase(), foreignKey.getForeignKeyTable().getName().toLowerCase()};
            }
        }
    }


    @Override
    public boolean isSameObject(ForeignKey thisForeignKey, ForeignKey thatForeignKey, Database accordingTo, DatabaseObjectComparatorChain chain) {

        if ((thisForeignKey.getPrimaryKeyTable() == null) || (thisForeignKey.getForeignKeyTable() == null) ||
                (thatForeignKey.getPrimaryKeyTable() == null) || (thatForeignKey.getForeignKeyTable() == null)) {
            //not all table information is set, have to rely on name

            if (thisForeignKey.getForeignKeyTable() != null && thisForeignKey.getForeignKeyTable().getName() != null &&
            		thatForeignKey.getForeignKeyTable() != null && thatForeignKey.getForeignKeyTable().getName()  != null)  {
                //FK names are not necessarily unique across all tables, so first check if FK tables are different
                if (!chain.isSameObject(thisForeignKey.getForeignKeyTable(), thatForeignKey.getForeignKeyTable(), accordingTo)) {
                    return false;
                }
            }

            if ((thisForeignKey.getName() != null) && (thatForeignKey.getName() != null)) {
                if(accordingTo.isCaseSensitive()) {
                    return thisForeignKey.getName().equals(thatForeignKey.getName());
                } else {
                    return thisForeignKey.getName().equalsIgnoreCase(thatForeignKey.getName());
                }
            } else {
                return false;
            }
        }

        if ((thisForeignKey.getForeignKeyColumns() != null) && (thisForeignKey.getPrimaryKeyColumns() != null) &&
                (thatForeignKey.getForeignKeyColumns() != null) && (thatForeignKey.getPrimaryKeyColumns() != null)) {
            boolean columnsTheSame;
            StringUtil.StringUtilFormatter<Column> formatter = obj -> obj.toString(false);

            if (accordingTo.isCaseSensitive()) {
                columnsTheSame = StringUtil.join(thisForeignKey.getForeignKeyColumns(), ",", formatter).equals(StringUtil.join(thatForeignKey.getForeignKeyColumns(), ",", formatter)) &&
                        StringUtil.join(thisForeignKey.getPrimaryKeyColumns(), ",", formatter).equals(StringUtil.join(thatForeignKey.getPrimaryKeyColumns(), ",", formatter));
            } else {
                columnsTheSame = StringUtil.join(thisForeignKey.getForeignKeyColumns(), ",", formatter).equalsIgnoreCase(StringUtil.join(thatForeignKey.getForeignKeyColumns(), ",", formatter)) &&
                        StringUtil.join(thisForeignKey.getPrimaryKeyColumns(), ",", formatter).equalsIgnoreCase(StringUtil.join(thatForeignKey.getPrimaryKeyColumns(), ",", formatter));
            }

            return columnsTheSame &&
                    DatabaseObjectComparatorFactory.getInstance().isSameObject(thisForeignKey.getForeignKeyTable(), thatForeignKey.getForeignKeyTable(), chain.getSchemaComparisons(), accordingTo) &&
                    DatabaseObjectComparatorFactory.getInstance().isSameObject(thisForeignKey.getPrimaryKeyTable(), thatForeignKey.getPrimaryKeyTable(), chain.getSchemaComparisons(), accordingTo);
        }

        return false;
    }

    @Override
    public ObjectDifferences findDifferences(ForeignKey thisForeignKey, ForeignKey thatForeignKey, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain chain, Set<String> exclue) {
        exclue.add("name");
        exclue.add("backingIndex");
        exclue.add("foreignKeyColumns");
        exclue.add("primaryKeyColumns");
        exclue.add("foreignKeyTable");
        exclue.add("primaryKeyTable");

        ObjectDifferences differences = chain.findDifferences(thisForeignKey, thatForeignKey, accordingTo, compareControl, exclue);
        differences.compare("foreignKeyColumns", thisForeignKey, thatForeignKey, new ObjectDifferences.DatabaseObjectNameCompareFunction(Column.class, accordingTo));
        differences.compare("primaryKeyColumns", thisForeignKey, thatForeignKey, new ObjectDifferences.DatabaseObjectNameCompareFunction(Column.class, accordingTo));
        differences.compare("foreignKeyTable", thisForeignKey, thatForeignKey, new ObjectDifferences.DatabaseObjectNameCompareFunction(Table.class, accordingTo));
        differences.compare("primaryKeyTable", thisForeignKey, thatForeignKey, new ObjectDifferences.DatabaseObjectNameCompareFunction(Table.class, accordingTo));
        return differences;
    }
}
