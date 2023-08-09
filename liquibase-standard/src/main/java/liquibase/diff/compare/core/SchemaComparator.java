package liquibase.diff.compare.core;

import liquibase.CatalogAndSchema;
import liquibase.GlobalConfiguration;
import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

import java.util.Set;

public class SchemaComparator extends CommonCatalogSchemaComparator<Schema> {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (Schema.class.isAssignableFrom(objectType)) {
            return PRIORITY_TYPE;
        }
        return PRIORITY_NONE;
    }

    @Override
    public String[] hash(Schema schema, Database accordingTo, DatabaseObjectComparatorChain chain) {
        return null;
    }

    @Override
    public boolean isSameObject(Schema thisSchema, Schema otherSchema, Database accordingTo, DatabaseObjectComparatorChain<Schema> chain) {

        String schemaName1 = null;
        String schemaName2 = null;

        // the flag will be set true in multi catalog environments
        boolean shouldIncludeCatalog = GlobalConfiguration.INCLUDE_CATALOG_IN_SPECIFICATION.getCurrentValue();
        if (shouldIncludeCatalog) {
            Catalog catalog1 = thisSchema.getCatalog();
            Catalog catalog2 = otherSchema.getCatalog();
            if (!DatabaseObjectComparatorFactory.getInstance().isSameObject(catalog1, catalog2, chain.getSchemaComparisons(), accordingTo)) {
                return false;
            }
        }
        if (accordingTo.supportsSchemas()) {
            schemaName1 = thisSchema.getName();
            schemaName2 = otherSchema.getName();

        } else if (accordingTo.supportsCatalogs()) {
            schemaName1 = thisSchema.getCatalogName();
            schemaName2 = otherSchema.getCatalogName();
        }

        if (equalsSchemas(accordingTo, schemaName1, schemaName2)) {
            return true;
        }

        //switch off default names and then compare again
        schemaName1 = getSchemaAfterComparison(accordingTo, schemaName1);
        schemaName2 = getSchemaAfterComparison(accordingTo, schemaName2);
        if (equalsSchemas(accordingTo, schemaName1, schemaName2)) {
            return true;
        }

        //check with schemaComparisons
        if ((chain.getSchemaComparisons() != null) && (chain.getSchemaComparisons().length > 0)) {
            for (CompareControl.SchemaComparison comparison : chain.getSchemaComparisons()) {
                String comparisonSchema1 = getComparisonSchemaOrCatalog(accordingTo, comparison);
                String comparisonSchema2 = getReferenceSchemaOrCatalog(accordingTo, comparison);
                String finalSchema1 = schemaName1;
                String finalSchema2 = schemaName2;

                finalSchema1 = getFinalSchemaAfterComparison(accordingTo, schemaName1, comparisonSchema1,
                        comparisonSchema2, finalSchema1);
                if (equalsSchemas(accordingTo, finalSchema1, finalSchema2)) {
                    return true;
                }
                finalSchema2 = getFinalSchemaAfterComparison(accordingTo, schemaName2, comparisonSchema2,
                        comparisonSchema1, finalSchema2);
                if (equalsSchemas(accordingTo, finalSchema1, finalSchema2)) {
                    return true;
                }
            }
        }

        schemaName1 = thisSchema.toCatalogAndSchema().standardize(accordingTo).getSchemaName();
        schemaName2 = otherSchema.toCatalogAndSchema().standardize(accordingTo).getSchemaName();

        return equalsSchemas(accordingTo, schemaName1, schemaName2);
    }

    private String getSchemaAfterComparison(Database accordingTo, String schemaName1) {
        if (schemaName1 == null) {
            if (accordingTo.supportsSchemas()) {
                schemaName1 = accordingTo.getDefaultSchemaName();
            } else if (accordingTo.supportsCatalogs()) {
                schemaName1 = accordingTo.getDefaultCatalogName();
            }
        }
        return schemaName1;
    }

    private String getFinalSchemaAfterComparison(Database accordingTo, String schemaName1, String comparisonSchema1, String comparisonSchema2, String finalSchema1) {
        if (CatalogAndSchema.CatalogAndSchemaCase.ORIGINAL_CASE.equals(accordingTo.getSchemaAndCatalogCase())){
            if ((comparisonSchema1 != null) && comparisonSchema1.equals(schemaName1)) {
                finalSchema1 = comparisonSchema2;
            } else if ((comparisonSchema2 != null) && comparisonSchema2.equals(schemaName1)) {
                finalSchema1 = comparisonSchema1;
            }
        } else {
            if ((comparisonSchema1 != null) && comparisonSchema1.equalsIgnoreCase(schemaName1)) {
                finalSchema1 = comparisonSchema2;
            } else if ((comparisonSchema2 != null) && comparisonSchema2.equalsIgnoreCase(schemaName1)) {
                finalSchema1 = comparisonSchema1;
            }
        }

        return finalSchema1;
    }

    @Override
    public ObjectDifferences findDifferences(Schema thisSchema, Schema otherSchema, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain<Schema> chain, Set<String> exclude) {
        ObjectDifferences differences = new ObjectDifferences(compareControl);
        differences.compare("name", thisSchema, otherSchema, new ObjectDifferences.DatabaseObjectNameCompareFunction(Schema.class, accordingTo));

        return differences;
    }
}
