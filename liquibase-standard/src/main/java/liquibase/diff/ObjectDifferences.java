package liquibase.diff;

import liquibase.database.Database;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.util.NumberUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static liquibase.diff.compare.core.DefaultDatabaseObjectComparator.compareObjectNames;

public class ObjectDifferences {

    private final CompareControl compareControl;
    private final HashMap<String, Difference> differences = new HashMap<>();

    public ObjectDifferences(CompareControl compareControl) {
        this.compareControl = compareControl;
    }

    public Set<Difference> getDifferences() {
        return Collections.unmodifiableSet(new TreeSet<>(differences.values()));
    }

    public Difference getDifference(String field) {
        return differences.get(field);
    }

    public boolean isDifferent(String field) {
        return differences.containsKey(field);
    }

    public ObjectDifferences addDifference(String changedField, Object referenceValue, Object compareToValue) {
        this.differences.put(changedField, new Difference(changedField, referenceValue, compareToValue));

        return this;
    }

    public ObjectDifferences addDifference(String message, String changedField, Object referenceValue, Object compareToValue) {
        this.differences.put(changedField, new Difference(message, changedField, referenceValue, compareToValue));

        return this;
    }

    public boolean hasDifferences() {
        return !differences.isEmpty();
    }

    public <T extends DatabaseObject<T>> void compare(String attribute, T referenceObject, T compareToObject, CompareFunction<Object> compareFunction) {
        compare(null, attribute, referenceObject, compareToObject, compareFunction);
    }

    public <T extends DatabaseObject<T>> void compare(String message, String attribute, T referenceObject, T compareToObject, CompareFunction<Object> compareFunction) {
        if (compareControl.isSuppressedField(referenceObject.getClass(), attribute)) {
            return;
        }

        Object referenceValue = referenceObject.getAttribute(attribute, referenceObject.getClass());
        Object compareValue = compareToObject.getAttribute(attribute, compareToObject.getClass());

        referenceValue = undoCollection(referenceValue, compareValue);
        compareValue = undoCollection(compareValue, referenceValue);

        boolean different;
        if ((referenceValue == null) && (compareValue == null)) {
            different = false;
        } else if (((referenceValue == null) && (compareValue != null)) || ((referenceValue != null) && (compareValue
            == null))) {
            different = true;
        } else {
            different = !compareFunction.areEqual(referenceValue, compareValue);
        }

        if (different) {
            addDifference(message, attribute, referenceValue, compareValue);
        }
    }

    /**
     * Sometimes an attribute in one object is a single-entity collection and on the other it is just the object.
     * Check the passed potentialCollection and if it is a single-entry collection of the same type as the otherObject, return just the collection element.
     * Otherwise, return the original collection.
     */
    protected Object undoCollection(Object potentialCollection, Object otherObject) {
        if ((otherObject != null) && (potentialCollection instanceof Collection) &&
                !(otherObject instanceof Collection)) {
            if ((((Collection<?>) potentialCollection).size() == 1) && ((Collection) potentialCollection).iterator()
                .next().getClass().equals(otherObject.getClass())) {
                potentialCollection = ((Collection) potentialCollection).iterator().next();
            }
        }
        return potentialCollection;
    }

    public boolean removeDifference(String attribute) {
        return differences.remove(attribute) != null;
    }

    public CompareControl.SchemaComparison[] getSchemaComparisons() {
        return compareControl.getSchemaComparisons();
    }

    public interface CompareFunction<T> {
        boolean areEqual(T referenceValue, T compareToValue);
    }

    public static class StandardCompareFunction implements CompareFunction<Object> {

        private final NumberCompareFunction numberCompareFunction;
        private final DatabaseObjectCompareFunction databaseObjectCompareFunction;

        public StandardCompareFunction(CompareControl.SchemaComparison[] schemaComparisons, Database accordingTo) {
            this.numberCompareFunction = new NumberCompareFunction();
            this.databaseObjectCompareFunction = new DatabaseObjectCompareFunction(accordingTo, schemaComparisons);
        }

        @Override
        public boolean areEqual(Object referenceValue, Object compareToValue) {

            if (referenceValue == null && compareToValue == null) {
                return true;
            }

            if (referenceValue == null || compareToValue == null) {
                return false;
            }

            if (referenceValue instanceof DatabaseObject && compareToValue instanceof DatabaseObject) {
            	return databaseObjectCompareFunction.areEqual((DatabaseObject<?>) referenceValue, (DatabaseObject<?>) compareToValue);
            }

            if (referenceValue instanceof Number && compareToValue instanceof Number) {
            	return numberCompareFunction.areEqual((Number) referenceValue, (Number) compareToValue);
            }

            return referenceValue.equals(compareToValue);
        }
    }
    
    public static class NumberCompareFunction implements CompareFunction<Number> {

        @Override
        public boolean areEqual(Number referenceValue, Number compareToValue) {

            if (referenceValue == null && compareToValue == null) {
                return true;
            }

            if (referenceValue == null || compareToValue == null) {
                return false;
            }
            
            return NumberUtil.numbersEqual(referenceValue, compareToValue);
        }
    }

    public static class DatabaseObjectCompareFunction implements CompareFunction<DatabaseObject<?>> {

        private final CompareControl.SchemaComparison[] schemaComparisons;
        private final Database accordingTo;

        public DatabaseObjectCompareFunction(Database accordingTo, CompareControl.SchemaComparison...schemaComparisons) {
            this.schemaComparisons = schemaComparisons == null ? new CompareControl.SchemaComparison[0] : schemaComparisons;
            this.accordingTo = accordingTo;
        }

        @Override
        public boolean areEqual(DatabaseObject<?> referenceValue, DatabaseObject<?> compareToValue) {

            if (referenceValue == null && compareToValue == null) {
                return true;
            }

            if (referenceValue == null || compareToValue == null) {
                return false;
            }

            return DatabaseObjectComparatorFactory.getInstance().isSameObject(referenceValue, compareToValue, schemaComparisons, accordingTo);
        }
    }

    public static class ToStringCompareFunction implements CompareFunction<Object> {

        private final boolean caseSensitive;

        public ToStringCompareFunction(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
        }

        @Override
        public boolean areEqual(Object referenceValue, Object compareToValue) {

            if (referenceValue == null && compareToValue == null) {
                return true;
            }

            if (referenceValue == null || compareToValue == null) {
                return false;
            }

            if (caseSensitive) {
                return referenceValue.toString().equals(compareToValue.toString());
            } else {
                return referenceValue.toString().equalsIgnoreCase(compareToValue.toString());
            }
        }
    }

    public static class DatabaseObjectNameCompareFunction implements CompareFunction<Object> {

        private final Database accordingTo;
        private final Class<? extends DatabaseObject> type;

        public DatabaseObjectNameCompareFunction(Class<? extends DatabaseObject> type, Database accordingTo) {
            this.type = type;
            this.accordingTo = accordingTo;
        }

        @Override
        public boolean areEqual(Object referenceValue, Object compareToValue) {

            if (referenceValue == null && compareToValue == null) {
                return true;
            }

            if (referenceValue == null || compareToValue == null) {
                return false;
            }

            if (referenceValue instanceof Collection) {
                if (!(compareToValue instanceof Collection)) {
                    return false;
                }
                if (((Collection<?>) referenceValue).size() != ((Collection<?>) compareToValue).size()) {
                    return false;
                } else {
                    Iterator referenceIterator = ((Collection) referenceValue).iterator();
                    Iterator compareToIterator = ((Collection) compareToValue).iterator();

                    while (referenceIterator.hasNext()) {
                        if (!areEqual(referenceIterator.next(), compareToIterator.next())) {
                            return false;
                        }
                    }
                    return true;
                }
            }

            String object1Name = getObject1Name(referenceValue);
            String object2Name = getObject1Name(compareToValue);

            return compareObjectNames(accordingTo, object1Name, object2Name);
        }

        private String getObject1Name(Object objectValue) {
            if (objectValue instanceof DatabaseObject) {
                return accordingTo.correctObjectName(((DatabaseObject<?>) objectValue).getAttribute("name", String.class), type);
            } else if (type.equals(Column.class)) {
                return accordingTo.correctObjectName(objectValue.toString(), type);
            }
            return objectValue.toString();
        }
    }

    public static class DataTypeCompareFunction implements CompareFunction {

        private final Database accordingTo;

        public DataTypeCompareFunction(Database accordingTo) {
            this.accordingTo = accordingTo;
        }

        @Override
        public boolean areEqual(Object referenceValue, Object compareToValue) {
            if ((referenceValue == null) && (compareToValue == null)) {
                return true;
            }
            if ((referenceValue == null) || (compareToValue == null)) {
                return false;
            }

            DataType referenceType = (DataType) referenceValue;
            DataType compareToType = (DataType) compareToValue;

            if (!referenceType.getTypeName().equalsIgnoreCase(compareToType.getTypeName())) {
                return false;
            }

            if (compareToType.toString().contains("(") && referenceType.toString().contains("(")) {
                return compareToType.toString().equalsIgnoreCase(referenceType.toString());
            } else {
                return true;
            }
        }
    }

    public static class OrderedCollectionCompareFunction implements CompareFunction<Collection<?>> {

        private final StandardCompareFunction compareFunction;

        public OrderedCollectionCompareFunction(StandardCompareFunction compareFunction) {
            this.compareFunction = compareFunction;
        }

        @Override
        public boolean areEqual(Collection<?> referenceValue, Collection<?> compareToValue) {

            if (referenceValue == null && compareToValue == null) {
                return true;
            }

            if (referenceValue == null || compareToValue == null) {
                return false;
            }

            if (referenceValue.size() != compareToValue.size()) {
                return false;
            }

            Iterator<?> referenceIterator = referenceValue.iterator();
            Iterator<?> compareIterator = compareToValue.iterator();

            while (referenceIterator.hasNext()) {
                Object referenceObj = referenceIterator.next();
                Object compareObj = compareIterator.next();

                if (!compareFunction.areEqual(referenceObj, compareObj)) {
                    return false;
                }
            }

            return true;
        }
    }

    public static class UnOrderedCollectionCompareFunction implements CompareFunction<Collection<?>> {

        private final StandardCompareFunction compareFunction;

        public UnOrderedCollectionCompareFunction(StandardCompareFunction compareFunction) {
            this.compareFunction = compareFunction;
        }

        @Override
        public boolean areEqual(Collection<?> referenceValue, Collection<?> compareToValue) {

            if (referenceValue == null && compareToValue == null) {
                return true;
            }

            if (referenceValue == null || compareToValue == null) {
                return false;
            }

            if (referenceValue.size() != compareToValue.size()) {
                return false;
            }

            for (Object referenceObj : referenceValue) {
                Object foundMatch = null;
                List<Object> unmatchedCompareToValues = new ArrayList<>(compareToValue);
                for (Object compareObj : unmatchedCompareToValues) {
                    if (compareFunction.areEqual(referenceObj, compareObj)) {
                        foundMatch = compareObj;
                        break;
                    }
                }
                if (foundMatch == null) {
                    return false;
                } else {
                    unmatchedCompareToValues.remove(foundMatch);
                }
            }

            return true;
        }
    }
}
