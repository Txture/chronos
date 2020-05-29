package org.chronos.chronodb.internal.impl.index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException;
import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.index.IndexManagerBackend;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractBackendDelegatingIndexManager<C extends ChronoDBInternal, B extends IndexManagerBackend>
    extends AbstractIndexManager<C> {

    // =====================================================================================================================
    // FIELDS
    // =====================================================================================================================

    private final B backend;

    protected final SetMultimap<String, Indexer<?>> indexNameToIndexers = HashMultimap.create();
    protected final Map<String, Boolean> indexNameToDirtyFlag = Maps.newHashMap();

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public AbstractBackendDelegatingIndexManager(final C owningDB, final B backend) {
        super(owningDB);
        checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
        this.backend = backend;
        // initialize the indexers by loading them from the backend
        SetMultimap<String, Indexer<?>> loadedIndexers = this.backend.loadIndexersFromPersistence();
        this.indexNameToIndexers.putAll(loadedIndexers);
        this.indexNameToDirtyFlag.clear();
        this.indexNameToDirtyFlag.putAll(this.backend.loadIndexStates());
    }

    // =================================================================================================================
    // INDEX MANAGEMENT
    // =================================================================================================================

    @Override
    public Set<String> getIndexNames() {
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            return Collections.unmodifiableSet(Sets.newHashSet(this.indexNameToIndexers.keySet()));
        }
    }

    @Override
    public Set<Indexer<?>> getIndexers() {
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            return Collections.unmodifiableSet(Sets.newHashSet(this.indexNameToIndexers.values()));
        }
    }

    @Override
    public Map<String, Set<Indexer<?>>> getIndexersByIndexName() {
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            Map<String, Collection<Indexer<?>>> map = this.indexNameToIndexers.asMap();
            Map<String, Set<Indexer<?>>> resultMap = Maps.newHashMap();
            for (Entry<String, Collection<Indexer<?>>> entry : map.entrySet()) {
                String indexName = entry.getKey();
                Collection<Indexer<?>> indexers = entry.getValue();
                Set<Indexer<?>> indexerSet = Collections.unmodifiableSet(Sets.newHashSet(indexers));
                resultMap.put(indexName, indexerSet);
            }
            return Collections.unmodifiableMap(resultMap);
        }
    }

    @Override
    public boolean isReindexingRequired() {
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            return this.indexNameToDirtyFlag.values().contains(true);
        }
    }

    @Override
    public Set<String> getDirtyIndices() {
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            // create a stream on the entry set of the map
            Set<String> dirtyIndices = this.indexNameToDirtyFlag.entrySet().stream()
                // keep only the entries where the dirty flag is true
                .filter(entry -> entry.getValue() == true)
                // from each entry, keep only the key
                .map(entry -> entry.getKey())
                // put the keys in a set
                .collect(Collectors.toSet());
            return Collections.unmodifiableSet(dirtyIndices);
        }
    }

    @Override
    public void removeIndex(final String indexName) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        this.getOwningDB().getConfiguration().assertNotReadOnly();
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            this.backend.deleteIndexAndIndexers(indexName);
            this.indexNameToIndexers.removeAll(indexName);
            this.clearQueryCache();
        }
    }

    @Override
    public void clearAllIndices() {
        this.getOwningDB().getConfiguration().assertNotReadOnly();
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            this.backend.deleteAllIndicesAndIndexers();
            this.indexNameToIndexers.clear();
            this.clearQueryCache();
        }
    }

    @Override
    public void addIndexer(final String indexName, final StringIndexer indexer) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
        this.addIndexerInternal(indexName, indexer);
    }

    @Override
    public void addIndexer(final String indexName, final LongIndexer indexer) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
        this.addIndexerInternal(indexName, indexer);
    }

    @Override
    public void addIndexer(final String indexName, final DoubleIndexer indexer) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
        this.addIndexerInternal(indexName, indexer);
    }

    @Override
    public void removeIndexer(final Indexer<?> indexer) {
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            Set<String> indexNamesContainingTheIndexer = Sets.newHashSet();
            for (String indexName : this.getIndexNames()) {
                if (this.indexNameToIndexers.get(indexName).contains(indexer)) {
                    indexNamesContainingTheIndexer.add(indexName);
                }
            }
            for (String indexName : indexNamesContainingTheIndexer) {
                boolean removed = this.indexNameToIndexers.remove(indexName, indexer);
                if (removed) {
                    this.setIndexDirty(indexName);
                }
            }
            // TODO PERFORMANCE: this is very inefficient on SQL backend. Can it be avoided?
            this.backend.persistIndexers(HashMultimap.create(this.indexNameToIndexers));
            this.clearQueryCache();
        }
    }

    @Override
    public void markAllIndicesAsDirty() {
        try(AutoLock lock = this.getOwningDB().lockExclusive()){
            boolean changed = false;
            for(String indexName : this.getIndexNames()){
                Boolean previous = this.indexNameToDirtyFlag.put(indexName, true);
                if(previous == null || previous == false){
                    // index was not dirty before
                    changed = true;
                }
            }
            if(changed){
                this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
            }
        }
    }

    // =====================================================================================================================
    // ROLLBACK METHODS
    // =====================================================================================================================

    @Override
    public void rollback(final long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        this.getOwningDB().getConfiguration().assertNotReadOnly();
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            Set<String> branchNames = this.getOwningDB().getBranchManager().getBranchNames();
            this.backend.rollback(branchNames, timestamp);
            this.clearQueryCache();
        }
    }

    @Override
    public void rollback(final Branch branch, final long timestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        this.getOwningDB().getConfiguration().assertNotReadOnly();
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            this.backend.rollback(Collections.singleton(branch.getName()), timestamp);
            this.clearQueryCache();
        }
    }

    @Override
    public void rollback(final Branch branch, final long timestamp, final Set<QualifiedKey> keys) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(keys, "Precondition violation - argument 'keys' must not be NULL!");
        this.getOwningDB().getConfiguration().assertNotReadOnly();
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            this.backend.rollback(Collections.singleton(branch.getName()), timestamp, keys);
        }
    }

    // =====================================================================================================================
    // INTERNAL HELPER METHODS
    // =====================================================================================================================

    protected B getIndexManagerBackend() {
        return this.backend;
    }

    protected void setIndexDirty(final String indexName) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        Boolean previous = this.indexNameToDirtyFlag.put(indexName, true);
        if (previous == null || previous == false) {
            this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
        }
    }

    protected void setIndexClean(final String indexName) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        Boolean previous = this.indexNameToDirtyFlag.put(indexName, false);
        if (previous == null || previous == true) {
            this.backend.persistIndexDirtyStates(this.indexNameToDirtyFlag);
        }
    }

    protected void addIndexerInternal(final String indexName, final Indexer<?> indexer) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
        this.getOwningDB().getConfiguration().assertNotReadOnly();
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            // check the other indexers on this index and make sure that indexer types cannot be mixed
            // on the same index name.
            this.assertAddingIndexersDoesNotProduceMixedIndex(indexName, indexer);
            this.assertHashCodeAndEqualsAreImplemented(indexName, indexer);
            this.assertNoIndexerDuplicates(indexName, indexer);
            this.backend.persistIndexer(indexName, indexer);
            this.indexNameToIndexers.put(indexName, indexer);
            this.setIndexDirty(indexName);
            this.clearQueryCache();
        }
    }

    private void assertAddingIndexersDoesNotProduceMixedIndex(final String indexName, final Indexer<?> indexerToAdd) {
        IndexType indexType = this.getIndexType(indexName);
        Class<? extends Indexer<?>> indexerType = this.getIndexerType(indexerToAdd);
        if (indexType != null) {
            boolean error = false;
            switch (indexType) {
                case STRING:
                    if (indexerType.equals(StringIndexer.class) == false) {
                        // cannot add non-string-based indexer to string-based index
                        error = true;
                    }
                    break;
                case LONG:
                    if (indexerType.equals(LongIndexer.class) == false) {
                        // cannot add non-long-based indexer to long-based index
                        error = true;
                    }
                    break;
                case DOUBLE:
                    if (indexerType.equals(DoubleIndexer.class) == false) {
                        // cannot add non-double-based indexer to double-based index
                        error = true;
                    }
                    break;
                default:
                    throw new UnknownEnumLiteralException(indexerType);
            }
            if (error) {
                throw new ChronoDBIndexingException("Cannot add " + indexerType.getSimpleName() + " '" + indexerToAdd.getClass().getName() + "' to index '" + indexName + "', because this index is of type '" + indexType + "'. Do not mix indexer types on the same index name.");
            }
        }
    }

    @SuppressWarnings({"ConstantConditions", "EqualsWithItself"})
    private void assertHashCodeAndEqualsAreImplemented(String indexName, Indexer<?> indexerToAdd){
        boolean foundHashCodeOverride = false;
        boolean foundEqualsOverride = false;
        Class<?> currentClass = indexerToAdd.getClass();
        while(currentClass != null && currentClass.equals(Object.class) == false){
            try{
                Method equalsMethod = currentClass.getMethod("equals", Object.class);
                if(equalsMethod != null && !equalsMethod.getDeclaringClass().equals(Object.class)){
                    foundEqualsOverride = true;
                }
            } catch (NoSuchMethodException e) {
                // equals() is not overridden here, check parent class
            } catch(SecurityException e){
                // reflection not allowed, can't perform this check
                return;
            }
            try{
                Method hashCodeMethod = currentClass.getMethod("hashCode");
                if(hashCodeMethod != null && !hashCodeMethod.getDeclaringClass().equals(Object.class)){
                    foundHashCodeOverride = true;
                }
            } catch (NoSuchMethodException e) {
                // hashCode() is not overridden here, check parent class
            } catch(SecurityException e){
                // reflection not allowed, can't perform this check
                return;
            }
            currentClass = currentClass.getSuperclass();
        }
        if(!foundHashCodeOverride || !foundEqualsOverride){
            Class<? extends Indexer<?>> indexerType = this.getIndexerType(indexerToAdd);
            throw new ChronoDBIndexingException("Cannot add " + indexerType.getSimpleName() + " '" + indexerToAdd.getClass().getName() + "' to index '" + indexName + "', because it does not implement hashCode() and/or equals(Object). Please provide suitable implementations in your indexer class.");
        }
        // do a VERY basic check on the functionality of the equals(...) method:
        // assert that the object is equal to itself.
        if(indexerToAdd.equals(indexerToAdd) == false){
            Class<? extends Indexer<?>> indexerType = this.getIndexerType(indexerToAdd);
            throw new ChronoDBIndexingException("Cannot add " + indexerType.getSimpleName() + " '" + indexerToAdd.getClass().getName() + "' to index '" + indexName + "', because its equals(Object) method is broken - the indexer is not equal to itself. Please fix this in your indexer implementation.");
        }
    }

    private void assertNoIndexerDuplicates(final String indexName, final Indexer<?> indexerToAdd){
        Collection<Indexer<?>> existingIndexers = this.indexNameToIndexers.get(indexName);
        // if any of the existing indexers is equal (w.r.t. Object.equals(...)) to the new indexer, we reject it
        if(existingIndexers.contains(indexerToAdd)){
            Class<? extends Indexer<?>> indexerType = this.getIndexerType(indexerToAdd);
            throw new ChronoDBIndexingException("Cannot add " + indexerType.getSimpleName() + " '" + indexerToAdd.getClass().getName() + "' to index '" + indexName + "', because this index already contains another indexer which is equal to the new indexer. This would lead to indexer conflicts in the future, therefore this new indexer has not been added and was rejected instead.");
        }
    }

    private IndexType getIndexType(final String indexName) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            Set<Indexer<?>> existingIndexers = this.getIndexersByIndexName().get(indexName);
            if (existingIndexers != null) {
                boolean hasStringIndexer = false;
                boolean hasLongIndexer = false;
                boolean hasDoubleIndexer = false;
                for (Indexer<?> i : existingIndexers) {
                    if (i instanceof StringIndexer) {
                        hasStringIndexer = true;
                    } else if (i instanceof LongIndexer) {
                        hasLongIndexer = true;
                    } else if (i instanceof DoubleIndexer) {
                        hasDoubleIndexer = true;
                    } else {
                        throw new IllegalStateException("Unknown indexer type: '" + i.getClass().getName() + "'!");
                    }
                }
                if (hasStringIndexer) {
                    return IndexType.STRING;
                }
                if (hasLongIndexer) {
                    return IndexType.LONG;
                }
                if (hasDoubleIndexer) {
                    return IndexType.DOUBLE;
                }
            }
            return null;
        }
    }

    private Class<? extends Indexer<?>> getIndexerType(final Indexer<?> indexer) {
        if (indexer instanceof StringIndexer) {
            return StringIndexer.class;
        } else if (indexer instanceof LongIndexer) {
            return LongIndexer.class;
        } else if (indexer instanceof DoubleIndexer) {
            return DoubleIndexer.class;
        } else {
            throw new IllegalArgumentException("Unknown Indexer subclass: '" + indexer.getClass().getName() + "'!");
        }
    }

    private enum IndexType {

        STRING, LONG, DOUBLE;

        @Override
        public String toString() {
            switch (this) {
                case STRING:
                    return "String";
                case LONG:
                    return "Long";
                case DOUBLE:
                    return "Double";
                default:
                    return super.toString();
            }
        }
    }
}
