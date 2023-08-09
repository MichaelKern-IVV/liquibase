package liquibase.diff;

import liquibase.database.Database;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.DataType;

import java.math.BigDecimal;
import java.util.*;

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

    public <T extends DatabaseObject<T>> void compare(String attribute, T referenceObject, T compareToObject, CompareFunction<T> compareFunction) {
        compare(null, attribute, referenceObject, compareToObject, compareFunction);
    }

    public <T extends DatabaseObject<T>> void compare(String message, String attribute, T referenceObject, T compareToObject, CompareFunction<T> compareFunction) {
        if (compareControl.isSuppressedField(referenceObject.getClass(), attribute)) {
            return;
        }

        T referenceValue = (T) referenceObject.getAttribute(attribute, referenceObject.getClass());
        T compareValue =  (T) compareToObject.getAttribute(attribute, compareToObject.getClass());

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
    protected <T extends DatabaseObject<T>> T undoCollection(T potentialCollection, T otherObject) {
        if ((otherObject != null) && (potentialCollection instanceof Collection) &&
                !(otherObject instanceof Collection)) {
            if ((((Collection<?>) potentialCollection).size() == 1) && ((Collection) potentialCollection).iterator()
                .next().getClass().equals(otherObject.getClass())) {
                potentialCollection = (T) ((Collection) potentialCollection).iterator().next();
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

    public static class StandardCompareFunction<T extends DatabaseObject<T>> implements CompareFunction<T> {

        private final CompareControl.SchemaComparison[] schemaComparisons;
        private final Database accordingTo;

        public StandardCompareFunction(CompareControl.SchemaComparison[] schemaComparisons, Database accordingTo) {
            this.schemaComparisons = schemaComparisons;
            this.accordingTo = accordingTo;
        }

        @Override
        public boolean areEqual(T referenceValue, T compareToValue) {
            if ((referenceValue == null) && (compareToValue == null)) {
                return true;
            }
            if ((referenceValue == null) || (compareToValue == null)) {
                return false;
            }

            if ((referenceValue instanceof DatabaseObject) && (compareToValue instanceof DatabaseObject)) {
                return DatabaseObjectComparatorFactory.getInstance().isSameObject((DatabaseObject) referenceValue, (DatabaseObject) compareToValue, schemaComparisons, accordingTo);
            } else {
                if ((referenceValue instanceof Number) && (referenceValue instanceof Comparable)) {
                    return (compareToValue instanceof Number)
                        && (((Comparable) referenceValue).compareTo(compareToValue) == 0);
                } else {
                    return referenceValue.equals(compareToValue);
                }
            }
        }
    }

    public static class ToStringCompareFunction implements CompareFunction {

        private final boolean caseSensitive;

        public ToStringCompareFunction(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
        }

        @Override
        public boolean areEqual(Object referenceValue, Object compareToValue) {
            if ((referenceValue == null) && (compareToValue == null)) {
                return true;
            }
            if ((referenceValue == null) || (compareToValue == null)) {
                return false;
            }

            if (caseSensitive) {
                return referenceValue.toString().equals(compareToValue.toString());
            } else {
                return referenceValue.toString().equalsIgnoreCase(compareToValue.toString());
            }

        }
    }

    public static class DatabaseObjectNameCompareFunction<T extends DatabaseObject<?>> implements CompareFunction {

        private final Database accordingTo;
        private final Class<T> type;

        public DatabaseObjectNameCompareFunction(Class<T> type, Database accordingTo) {
            this.type = type;
            this.accordingTo = accordingTo;
        }

        @Override
        public boolean areEqual(Object referenceValue, Object compareToValue) {
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

            if ((referenceValue == null) && (compareToValue == null)) {
                return true;
            }
            if ((referenceValue == null) || (compareToValue == null)) {
                return false;
            }


            String object1Name;
            if (referenceValue instanceof DatabaseObject) {
                object1Name = accordingTo.correctObjectName(((DatabaseObject<?>) referenceValue).getAttribute("name", String.class), type);
            } else {
                object1Name = referenceValue.toString();
            }

            String object2Name;
            if (compareToValue instanceof DatabaseObject) {
                object2Name = accordingTo.correctObjectName(((DatabaseObject<?>) compareToValue).getAttribute("name", String.class), type);
            } else {
                object2Name = compareToValue.toString();
            }

            if ((object1Name == null) && (object2Name == null)) {
                return true;
            }
            if ((object1Name == null) || (object2Name == null)) {
                return false;
            }
            if (accordingTo.isCaseSensitive()) {
                return object1Name.equals(object2Name);
            } else {
                return object1Name.equalsIgnoreCase(object2Name);
            }
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

    public static class OrderedCollectionCompareFunction<T extends DatabaseObject<T>> implements CompareFunction<Collection<T>> {

        private final StandardCompareFunction<T> compareFunction;

        public OrderedCollectionCompareFunction(StandardCompareFunction<T> compareFunction) {
            this.compareFunction = compareFunction;
        }

        @Override
        public boolean areEqual(Collection<T> referenceValue, Collection<T> compareToValue) {
            if ((referenceValue == null) && (compareToValue == null)) {
                return true;
            }
            if ((referenceValue == null) || (compareToValue == null)) {
                return false;
            }

            if (((Collection<?>) referenceValue).size() != ((Collection<?>) compareToValue).size()) {
                return false;
            }

            if (referenceValue.size() != compareToValue.size()) {
                return false;
            }

            Iterator<T> referenceIterator = referenceValue.iterator();
            Iterator<T> compareIterator = compareToValue.iterator();
            while (referenceIterator.hasNext()) {
                T referenceObj = referenceIterator.next();
                T compareObj = compareIterator.next();

                if (!compareFunction.areEqual(referenceObj, compareObj)) {
                    return false;
                }
            }

            return true;
        }
    }


    public static class UnOrderedCollectionCompareFunction<T extends DatabaseObject<T>> implements CompareFunction<Collection<T>> {

        private final StandardCompareFunction<T> compareFunction;

        public UnOrderedCollectionCompareFunction(StandardCompareFunction<T> compareFunction) {
            this.compareFunction = compareFunction;
        }

        @Override
        public boolean areEqual(Collection<T> referenceValue, Collection<T> compareToValue) {
            if ((referenceValue == null) && (compareToValue == null)) {
                return true;
            }
            if ((referenceValue == null) || (compareToValue == null)) {
                return false;
            }

            if (referenceValue.size() != compareToValue.size()) {
                return false;
            }

            for (T referenceObj : referenceValue) {
                T foundMatch = null;
                List<T> unmatchedCompareToValues = new ArrayList<>(compareToValue);
                for (T compareObj : unmatchedCompareToValues) {
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
