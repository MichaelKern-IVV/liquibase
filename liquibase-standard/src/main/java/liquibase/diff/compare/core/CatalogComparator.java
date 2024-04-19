package liquibase.diff.compare.core;

import liquibase.CatalogAndSchema;
import liquibase.GlobalConfiguration;
import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;
import liquibase.util.StringUtil;

import java.util.Set;

public class CatalogComparator extends CommonCatalogSchemaComparator<Catalog> {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (Catalog.class.isAssignableFrom(objectType)) {
            return PRIORITY_TYPE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public String[] hash(Catalog databaseObject, Database accordingTo, DatabaseObjectComparatorChain chain) {
        return null;
    }

    @Override
    public boolean isSameObject(Catalog thisCatalog, Catalog thatCatalog, Database accordingTo, DatabaseObjectComparatorChain chain) {

        if (!accordingTo.supports(Catalog.class)) {
            return true;
        }

        // the flag will be set true in multi catalog environments
        boolean shouldIncludeCatalog = GlobalConfiguration.INCLUDE_CATALOG_IN_SPECIFICATION.getCurrentValue();
        String object1Name;
        if (!shouldIncludeCatalog && thisCatalog.isDefault()) {
            object1Name = null;
        } else {
            object1Name = thisCatalog.getName();
        }

        String object2Name;
        if (!shouldIncludeCatalog && thatCatalog.isDefault()) {
            object2Name = null;
        } else {
            object2Name = thatCatalog.getName();
        }

        CatalogAndSchema thisSchema = new CatalogAndSchema(object1Name, null).standardize(accordingTo);
        CatalogAndSchema otherSchema = new CatalogAndSchema(object2Name, null).standardize(accordingTo);

        if (thisSchema.getCatalogName() == null) {
            return otherSchema.getCatalogName() == null;
        }

        if (equalsSchemas(accordingTo,object1Name,  object2Name)) return true;

        //check with schemaComparisons
        if ((chain.getSchemaComparisons() != null) && (chain.getSchemaComparisons().length > 0)) {
            for (CompareControl.SchemaComparison comparison : chain.getSchemaComparisons()) {
                String comparisonCatalog1 = getComparisonSchemaOrCatalog(accordingTo, comparison);
                String comparisonCatalog2 = getReferenceSchemaOrCatalog(accordingTo, comparison);

                String finalCatalog1 = thisSchema.getCatalogName();
                String finalCatalog2 = otherSchema.getCatalogName();

                if ((comparisonCatalog1 != null) && comparisonCatalog1.equalsIgnoreCase(finalCatalog1)) {
                    finalCatalog1 = comparisonCatalog2;
                } else if ((comparisonCatalog2 != null) && comparisonCatalog2.equalsIgnoreCase(finalCatalog1)) {
                    finalCatalog1 = comparisonCatalog1;
                }

                if (StringUtil.trimToEmpty(finalCatalog1).equalsIgnoreCase(StringUtil.trimToEmpty(finalCatalog2))) {
                    return true;
                }

                if ((comparisonCatalog1 != null) && comparisonCatalog1.equalsIgnoreCase(finalCatalog2)) {
                    finalCatalog2 = comparisonCatalog2;
                } else if ((comparisonCatalog2 != null) && comparisonCatalog2.equalsIgnoreCase(finalCatalog2)) {
                    finalCatalog2 = comparisonCatalog1;
                }

                if (StringUtil.trimToEmpty(finalCatalog1).equalsIgnoreCase(StringUtil.trimToEmpty(finalCatalog2))) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public ObjectDifferences findDifferences(Catalog thisCatalog, Catalog thatCatalog, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain<Catalog> chain, Set<String> exclude) {
        ObjectDifferences differences = new ObjectDifferences(compareControl);
        differences.compare("name", thisCatalog, thatCatalog, new ObjectDifferences.DatabaseObjectNameCompareFunction(Catalog.class, accordingTo));  // TODO: Schema.class ? 

        return differences;
    }
}
