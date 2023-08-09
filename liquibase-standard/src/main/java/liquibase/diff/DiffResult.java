package liquibase.diff;

import liquibase.database.Database;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Column;
import liquibase.structure.core.Schema;
import liquibase.util.BooleanUtil;

import java.io.IOException;
import java.util.*;

public class DiffResult {

    private final DatabaseSnapshot referenceSnapshot;
    private final DatabaseSnapshot comparisonSnapshot;

    private final CompareControl compareControl;

    private StringDiff productNameDiff;
    private StringDiff productVersionDiff;

    private final Set<DatabaseObject<?>> missingObjects = new HashSet<>();
    private final Set<DatabaseObject<?>> unexpectedObjects = new HashSet<>();
    private final Map<DatabaseObject<?>, ObjectDifferences> changedObjects = new HashMap<>();


    public DiffResult(DatabaseSnapshot referenceDatabaseSnapshot, DatabaseSnapshot comparisonDatabaseSnapshot, CompareControl compareControl) {
        this.referenceSnapshot = referenceDatabaseSnapshot;
        this.comparisonSnapshot = comparisonDatabaseSnapshot;
        this.compareControl = compareControl;
    }

    public DatabaseSnapshot getReferenceSnapshot() {
        return referenceSnapshot;
    }

    public DatabaseSnapshot getComparisonSnapshot() {
        return comparisonSnapshot;
    }

    public StringDiff getProductNameDiff() {
        return productNameDiff;
    }

    public void setProductNameDiff(StringDiff productNameDiff) {
        this.productNameDiff = productNameDiff;
    }

    public StringDiff getProductVersionDiff() {
        return productVersionDiff;
    }


    public void setProductVersionDiff(StringDiff productVersionDiff) {
        this.productVersionDiff = productVersionDiff;
    }

    public CompareControl getCompareControl() {
        return compareControl;
    }

    public Set<DatabaseObject<?>> getMissingObjects() {
        return missingObjects;
    }

    public Set<DatabaseObject<?>> getMissingObjects(Class<? extends DatabaseObject> type) {
        Set<DatabaseObject<?>> returnSet = new HashSet<>();
        for (DatabaseObject<?> obj : missingObjects) {
            if (type.isAssignableFrom(obj.getClass())) {
                returnSet.add(obj);
            }
        }
        return returnSet;
    }

    public SortedSet<DatabaseObject<?>> getMissingObjects(Class<? extends DatabaseObject> type, Comparator<DatabaseObject<?>> comparator) {
        TreeSet<DatabaseObject<?>> set = new TreeSet<>(comparator);
        set.addAll(getMissingObjects(type));
        return set;
    }

    public DatabaseObject<?> getMissingObject(DatabaseObject<?> example, CompareControl.SchemaComparison[] schemaComparisons) {
        Database accordingTo = getComparisonSnapshot().getDatabase();
        DatabaseObjectComparatorFactory comparator = DatabaseObjectComparatorFactory.getInstance();
        for (DatabaseObject<?> obj : getMissingObjects((Class<DatabaseObject>) example.getClass())) {
            if (comparator.isSameObject(obj, example, schemaComparisons, accordingTo)) {
                return obj;
            }
        }
        return null;
    }

    public void addMissingObject(DatabaseObject<?> obj) {
        if ((obj instanceof Column) && (BooleanUtil.isTrue(((Column) obj).getComputed()) || BooleanUtil.isTrue(((Column) obj).getDescending()))) {
            return; //not really missing, it's a virtual column
        }
        missingObjects.add(obj);
    }

    public Set<DatabaseObject<?>> getUnexpectedObjects() {
        return unexpectedObjects;
    }

    public Set<DatabaseObject<?>> getUnexpectedObjects(Class<? extends DatabaseObject> type) {
        Set<DatabaseObject<?>> returnSet = new HashSet<>();
        for (DatabaseObject<?> obj : unexpectedObjects) {
            if (type.isAssignableFrom(obj.getClass())) {
                returnSet.add(obj);
            }
        }
        return returnSet;
    }

    public SortedSet<DatabaseObject<?>> getUnexpectedObjects(Class<? extends DatabaseObject> type, Comparator<DatabaseObject<?>> comparator) {
        TreeSet<DatabaseObject<?>> set = new TreeSet<>(comparator);
        set.addAll(getUnexpectedObjects(type));
        return set;
    }

    public DatabaseObject<?> getUnexpectedObject(DatabaseObject example, CompareControl.SchemaComparison[] schemaComparisons) {
        Database accordingTo = this.getComparisonSnapshot().getDatabase();
        DatabaseObjectComparatorFactory comparator = DatabaseObjectComparatorFactory.getInstance();
        for (DatabaseObject<?> obj : getUnexpectedObjects((Class<DatabaseObject>) example.getClass())) {
            if (comparator.isSameObject(obj, example, schemaComparisons, accordingTo)) {
                return obj;
            }
        }
        return null;
    }

    public void addUnexpectedObject(DatabaseObject<?> obj) {
        unexpectedObjects.add(obj);
    }

    public Map<DatabaseObject<?>, ObjectDifferences> getChangedObjects() {
        return changedObjects;
    }

    public Map<DatabaseObject<?>, ObjectDifferences> getChangedObjects(Class<? extends DatabaseObject> type) {
        Map<DatabaseObject<?>,ObjectDifferences> returnSet = new HashMap<>();
        for (Map.Entry<DatabaseObject<?>, ObjectDifferences> obj : changedObjects.entrySet()) {
            if (type.isAssignableFrom(obj.getKey().getClass())) {
                returnSet.put(obj.getKey(), obj.getValue());
            }
        }
        return returnSet;
    }

    public SortedMap<DatabaseObject<?>, ObjectDifferences> getChangedObjects(Class<? extends DatabaseObject> type, Comparator<DatabaseObject<?>> comparator) {
        SortedMap<DatabaseObject<?>, ObjectDifferences> map = new TreeMap<>(comparator);
        map.putAll(getChangedObjects(type));
        return map;
    }

    public ObjectDifferences getChangedObject(DatabaseObject<?> example, CompareControl.SchemaComparison[] schemaComparisons) {
        Database accordingTo = this.getComparisonSnapshot().getDatabase();
        DatabaseObjectComparatorFactory comparator = DatabaseObjectComparatorFactory.getInstance();
        for (Map.Entry<DatabaseObject<?>, ObjectDifferences> entry : getChangedObjects(example.getClass()).entrySet()) {
            if (comparator.isSameObject(entry.getKey(), example, schemaComparisons, accordingTo)) {
                return entry.getValue();
            }
        }
        return null;
    }


    public void addChangedObject(DatabaseObject<?> obj, ObjectDifferences differences) {
        if ((obj instanceof Catalog) || (obj instanceof Schema)) {
            if ((differences.getSchemaComparisons() != null) && (differences.getDifferences().size() == 1) &&
                (differences.getDifference("name") != null)) {
                if ((obj instanceof Catalog) && this.getReferenceSnapshot().getDatabase().supportsSchemas()) { //still save name
                    changedObjects.put(obj, differences);
                    return;
                } else {
                    return;  //don't save name differences
                }
            }
        }
        changedObjects.put(obj, differences);
    }

    public boolean areEqual() throws DatabaseException, IOException {

        return missingObjects.isEmpty() && unexpectedObjects.isEmpty() && changedObjects.isEmpty();
    }

    public Set<Class<? extends DatabaseObject>> getComparedTypes() {
        return compareControl.getComparedTypes();
    }
}
