package org.chronos.chronograph.internal.impl.index;

import com.google.common.collect.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.builder.query.WhereBuilder;
import org.chronos.chronodb.api.exceptions.UnknownKeyspaceException;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.query.*;
import org.chronos.chronodb.internal.api.query.searchspec.*;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.api.index.IChronoGraphEdgeIndex;
import org.chronos.chronograph.internal.api.index.IChronoGraphVertexIndex;
import org.chronos.chronograph.internal.impl.builder.index.ChronoGraphIndexBuilder;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphIndexManagerImpl implements ChronoGraphIndexManager, ChronoGraphIndexManagerInternal {

    // =====================================================================================================================
    // FIELDS
    // =====================================================================================================================

    private final ChronoDB chronoDB;
    private final String branchName;

    private final Map<String, IChronoGraphVertexIndex> vertexIndices;
    private final Map<String, IChronoGraphEdgeIndex> edgeIndices;

    private final ReadWriteLock lock;

    public ChronoGraphIndexManagerImpl(final ChronoDB chronoDB, final String branchName) {
        checkNotNull(chronoDB, "Precondition violation - argument 'chronoDB' must not be NULL!");
        this.chronoDB = chronoDB;
        this.branchName = branchName;
        this.vertexIndices = Maps.newHashMap();
        this.edgeIndices = Maps.newHashMap();
        this.lock = new ReentrantReadWriteLock(true);
        this.loadIndexDataFromDB();
    }

    // =====================================================================================================================
    // INDEX CREATION
    // =====================================================================================================================

    @Override
    public IndexBuilderStarter create() {
        return new ChronoGraphIndexBuilder(this);
    }

    // =====================================================================================================================
    // INDEX METADATA QUERYING
    // =====================================================================================================================

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesOf(final Class<? extends Element> clazz) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        return this.performNonExclusive(() -> {
            if (Vertex.class.isAssignableFrom(clazz)) {
                return Collections.unmodifiableSet(Sets.newHashSet(this.vertexIndices.values()));
            } else if (Edge.class.isAssignableFrom(clazz)) {
                return Collections.unmodifiableSet(Sets.newHashSet(this.edgeIndices.values()));
            } else {
                throw new IllegalArgumentException("Unknown graph element class: '" + clazz.getName() + "'!");
            }
        });
    }

    @Override
    public boolean isReindexingRequired() {
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().isReindexingRequired());
    }

    @Override
    public Set<ChronoGraphIndex> getDirtyIndices() {
        return this.performNonExclusive(() -> {
            IndexManager indexManager = this.getChronoDBIndexManager();
            Set<String> dirtyBackendIndexKeys = indexManager.getDirtyIndices();
            Set<ChronoGraphIndex> dirtyGraphIndices = Sets.newHashSet();
            for (String dirtyBackendIndexKey : dirtyBackendIndexKeys) {
                ChronoGraphIndex graphIndex = this.getIndexForBackendPropertyKey(dirtyBackendIndexKey);
                dirtyGraphIndices.add(graphIndex);
            }
            return Collections.unmodifiableSet(dirtyGraphIndices);
        });
    }

    // =====================================================================================================================
    // INDEX MANIPULATION
    // =====================================================================================================================

    @Override
    public void reindexAll(boolean force) {
        this.performExclusive(() -> this.getChronoDBIndexManager().reindexAll(force));
    }

    @Override
    public void reindex(final ChronoGraphIndex index) {
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        this.reindexAll();
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index) {
        this.dropIndex(index, null);
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index, final Object commitMetadata) {
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        this.performExclusive(() -> {
            ChronoGraphIndexInternal indexInternal = (ChronoGraphIndexInternal) index;
            IndexManager indexManager = this.getChronoDBIndexManager();
            indexManager.removeIndex(indexInternal.getBackendIndexKey());
            // FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
            ChronoDB db = this.chronoDB;
            ChronoDBTransaction tx = db.tx(this.branchName);
            tx.remove(indexInternal.getBackendIndexKey());
            tx.commit(commitMetadata);
            // FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
            if (indexInternal instanceof IChronoGraphVertexIndex) {
                this.vertexIndices.remove(indexInternal.getBackendIndexKey());
            } else if (indexInternal instanceof IChronoGraphEdgeIndex) {
                this.edgeIndices.remove(indexInternal.getBackendIndexKey());
            }
        });
    }


    @Override
    public void dropAllIndices() {
        this.dropAllIndices(null);
    }

    @Override
    public void dropAllIndices(final Object commitMetadata) {
        this.performExclusive(() -> {
            IndexManager indexManager = this.getChronoDBIndexManager();
            indexManager.clearAllIndices();
            // FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
            ChronoDB db = this.chronoDB;
            ChronoDBTransaction tx = db.tx(this.branchName);
            for (ChronoGraphIndex index : this.getAllIndices()) {
                tx.remove(((ChronoGraphIndexInternal) index).getBackendIndexKey());
            }
            tx.commit(commitMetadata);
            this.vertexIndices.clear();
            this.edgeIndices.clear();
        });
    }

    // =====================================================================================================================
    // INTERNAL API :: MODIFICATION
    // =====================================================================================================================

    @Override
    public void addIndex(final ChronoGraphIndex index) {
        this.addIndex(index, null);
    }

    @Override
    public void addIndex(final ChronoGraphIndex index, final Object commitMetadata) {
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        this.performExclusive(() -> {
            if (this.getAllIndices().contains(index)) {
                throw new IllegalArgumentException("The given index already exists: " + index.toString());
            }
            ChronoGraphIndexInternal indexInternal = (ChronoGraphIndexInternal) index;
            Indexer<?> indexer = indexInternal.createIndexer();
            IndexManager indexManager = this.getChronoDBIndexManager();
            indexManager.addIndexer(indexInternal.getBackendIndexKey(), indexer);
            // FIXME CONSISTENCY: What happens if an exception occurs at this line (or JVM shutdown, or...)?
            ChronoDB db = this.chronoDB;
            ChronoDBTransaction tx = db.tx(this.branchName);
            tx.put(ChronoGraphConstants.KEYSPACE_MANAGEMENT_INDICES, indexInternal.getBackendIndexKey(), indexInternal);
            tx.commit(commitMetadata);
            if (indexInternal instanceof IChronoGraphVertexIndex) {
                this.vertexIndices.put(indexInternal.getBackendIndexKey(), (IChronoGraphVertexIndex) indexInternal);
            } else if (indexInternal instanceof IChronoGraphEdgeIndex) {
                this.edgeIndices.put(indexInternal.getBackendIndexKey(), (IChronoGraphEdgeIndex) indexInternal);
            } else {
                throw new IllegalArgumentException(
                    "Unknown index class: '" + indexInternal.getClass().getName() + "'!");
            }
        });
    }

    // =====================================================================================================================
    // INTERNAL API :: SEARCH
    // =====================================================================================================================

    @Override
    public Iterator<String> findVertexIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?, ?>> searchSpecifications) {
        checkNotNull(searchSpecifications,
            "Precondition violation - argument 'searchSpecifications' must not be NULL!");
        return this.findElementsByIndexedProperties(tx, Vertex.class, ChronoGraphConstants.KEYSPACE_VERTEX,
            searchSpecifications);
    }

    @Override
    public Iterator<String> findEdgeIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?, ?>> searchSpecifications) {
        checkNotNull(searchSpecifications,
            "Precondition violation - argument 'searchSpecifications' must not be NULL!");
        return this.findElementsByIndexedProperties(tx, Edge.class, ChronoGraphConstants.KEYSPACE_EDGE,
            searchSpecifications);
    }

    private Iterator<String> findElementsByIndexedProperties(final ChronoGraphTransaction tx, final Class<? extends Element> clazz,
                                                             final String keyspace, final Set<SearchSpecification<?, ?>> searchSpecifications) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkNotNull(keyspace, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(searchSpecifications,
            "Precondition violation - argument 'searchSpecifications' must not be NULL!");
        // we need to make sure that all of the given properties are indeed indexed
        checkArgument(searchSpecifications.isEmpty() == false,
            "Precondition violation - need at least one search specification to search for!");
        Set<String> properties = searchSpecifications.stream().map(SearchSpecification::getProperty).collect(Collectors.toSet());
        this.assertAllPropertiesAreIndexed(clazz, properties);
        // build a map from 'backend property key' to 'search specifications'
        SetMultimap<String, SearchSpecification<?, ?>> backendPropertyKeyToSearchSpecs = HashMultimap.create();
        Set<ChronoGraphIndex> graphIndices = this.getIndexedPropertiesOf(clazz);
        for (SearchSpecification<?, ?> searchSpec : searchSpecifications) {
            String propertyName = searchSpec.getProperty();
            ChronoGraphIndex index = graphIndices.stream()
                .filter(idx -> idx.getIndexedProperty().equals(propertyName))
                .findAny()
                .orElseThrow(() ->
                    new IllegalStateException("Unable to find graph index for property name '" + propertyName + "'!")
                );
            String backendPropertyKey = ((ChronoGraphIndexInternal) index).getBackendIndexKey();
            backendPropertyKeyToSearchSpecs.put(backendPropertyKey, searchSpec);
        }
        // get the transaction
        ChronoDBTransaction backendTransaction = tx.getBackingDBTransaction();
        // build the composite query
        QueryBuilder builder = backendTransaction.find().inKeyspace(keyspace);
        FinalizableQueryBuilder finalizableBuilder = null;
        List<String> propertyList = Lists.newArrayList(backendPropertyKeyToSearchSpecs.keySet());
        for (int i = 0; i < propertyList.size(); i++) {
            String propertyName = propertyList.get(i);
            Set<SearchSpecification<?, ?>> searchSpecs = backendPropertyKeyToSearchSpecs.get(propertyName);
            FinalizableQueryBuilder innerTempBuilder = null;
            for (SearchSpecification<?, ?> searchSpec : searchSpecs) {
                WhereBuilder whereBuilder;
                if (innerTempBuilder == null) {
                    whereBuilder = builder.where(propertyName);
                } else {
                    whereBuilder = innerTempBuilder.and().where(propertyName);
                }
                innerTempBuilder = this.applyCondition(whereBuilder, searchSpec);
            }

            if (i + 1 < propertyList.size()) {
                // continuation
                builder = innerTempBuilder.and();
            } else {
                // done
                finalizableBuilder = innerTempBuilder;
            }
        }
        // run the query
        Iterator<QualifiedKey> keys = finalizableBuilder.getKeys();
        // this is the "raw" iterator over vertex IDs which we obtain from our index.
        return Iterators.transform(keys, QualifiedKey::getKey);
    }

    // =====================================================================================================================
    // INTERNAL HELPER METHODS
    // =====================================================================================================================


    private IndexManager getChronoDBIndexManager() {
        return this.chronoDB.getIndexManager();
    }

    private void loadIndexDataFromDB() {
        ChronoDBTransaction tx = this.chronoDB.tx(this.branchName);
        Set<String> allIndexKeys = null;
        try {
            allIndexKeys = tx.keySet(ChronoGraphConstants.KEYSPACE_MANAGEMENT_INDICES);
        } catch (UnknownKeyspaceException e) {
            // we do not yet have any indices
            allIndexKeys = Collections.emptySet();
        }
        for (String indexKey : allIndexKeys) {
            ChronoGraphIndex index = tx.get(ChronoGraphConstants.KEYSPACE_MANAGEMENT_INDICES, indexKey);
            if (index == null) {
                throw new IllegalStateException("Could not find index specification for index key '" + indexKey + "'!");
            }
            index = IndexMigrationUtil.migrate(index);
            if (index instanceof IChronoGraphVertexIndex) {
                IChronoGraphVertexIndex vertexIndex = (IChronoGraphVertexIndex) index;
                this.vertexIndices.put(vertexIndex.getBackendIndexKey(), vertexIndex);
            } else if (index instanceof IChronoGraphEdgeIndex) {
                IChronoGraphEdgeIndex edgeIndex = (IChronoGraphEdgeIndex) index;
                this.edgeIndices.put(edgeIndex.getBackendIndexKey(), edgeIndex);
            } else {
                throw new IllegalStateException(
                    "Loaded unknown graph index class: '" + index.getClass().getName() + "'!");
            }
        }
    }

    private ChronoGraphIndex getIndexForBackendPropertyKey(final String propertyKey) {
        checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
        if (propertyKey.startsWith(ChronoGraphConstants.INDEX_PREFIX_VERTEX)) {
            return this.vertexIndices.get(propertyKey);
        } else if (propertyKey.startsWith(ChronoGraphConstants.INDEX_PREFIX_EDGE)) {
            return this.edgeIndices.get(propertyKey);
        } else {
            throw new IllegalArgumentException("The string '" + propertyKey + "' is no valid index property key!");
        }
    }

    private void performExclusive(final Runnable r) {
        this.lock.writeLock().lock();
        try {
            r.run();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unused")
    private <T> T performExclusive(final Callable<T> c) {
        this.lock.writeLock().lock();
        try {
            return c.call();
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while performing exclusive task: " + e.toString(), e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private <T> T performNonExclusive(final Callable<T> c) {
        this.lock.readLock().lock();
        try {
            return c.call();
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while performing exclusive task: " + e.toString(), e);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    private void assertAllPropertiesAreIndexed(final Class<? extends Element> clazz, final Set<String> propertyNames) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkNotNull(propertyNames, "Precondition violation - argument 'propertyNames' must not be NULL!");
        if (Vertex.class.isAssignableFrom(clazz)) {
            Set<ChronoGraphIndex> indexedVertexProperties = this.getIndexedVertexProperties();
            Set<String> indexedVertexPropertyNames = indexedVertexProperties.stream()
                .map(ChronoGraphIndex::getIndexedProperty).collect(Collectors.toSet());
            Set<String> unindexedProperties = Sets.newHashSet(propertyNames);
            unindexedProperties.removeAll(indexedVertexPropertyNames);
            if (unindexedProperties.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Some of the given properties are not indexed on vertices: " + unindexedProperties);
            }
        } else if (Edge.class.isAssignableFrom(clazz)) {
            Set<ChronoGraphIndex> indexedEdgeProperties = this.getIndexedEdgeProperties();
            Set<String> indexedEdgePropertyNames = indexedEdgeProperties.stream().map(ChronoGraphIndex::getIndexedProperty)
                .collect(Collectors.toSet());
            Set<String> unindexedProperties = Sets.newHashSet(propertyNames);
            unindexedProperties.removeAll(indexedEdgePropertyNames);
            if (unindexedProperties.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Some of the given properties are not indexed on edges: " + unindexedProperties);
            }
        } else {
            throw new IllegalArgumentException("Unknown graph element class: '" + clazz.getName() + "'!");
        }
    }

    private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
                                                   final SearchSpecification<?, ?> searchSpec) {
        if (searchSpec instanceof StringSearchSpecification) {
            return this.applyCondition(whereBuilder, (StringSearchSpecification) searchSpec);
        } else if (searchSpec instanceof LongSearchSpecification) {
            return this.applyCondition(whereBuilder, (LongSearchSpecification) searchSpec);
        } else if (searchSpec instanceof DoubleSearchSpecification) {
            return this.applyCondition(whereBuilder, (DoubleSearchSpecification) searchSpec);
        } else if (searchSpec instanceof ContainmentStringSearchSpecification) {
            return this.applyCondition(whereBuilder, (ContainmentStringSearchSpecification) searchSpec);
        } else if (searchSpec instanceof ContainmentLongSearchSpecification) {
            return this.applyCondition(whereBuilder, (ContainmentLongSearchSpecification) searchSpec);
        } else if (searchSpec instanceof ContainmentDoubleSearchSpecification) {
            return this.applyCondition(whereBuilder, (ContainmentDoubleSearchSpecification) searchSpec);
        } else {
            throw new IllegalStateException(
                "Unknown SearchSpecification class: '" + searchSpec.getClass().getName() + "'!");
        }
    }

    private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
                                                   final StringSearchSpecification searchSpec) {
        StringCondition condition = searchSpec.getCondition();
        TextMatchMode matchMode = searchSpec.getMatchMode();
        String searchText = searchSpec.getSearchValue();
        if (condition.equals(StringCondition.CONTAINS)) {
            return this.applyContainsCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.ENDS_WITH)) {
            return this.applyEndsWithCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.EQUALS)) {
            return this.applyEqualsCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.MATCHES_REGEX)) {
            return this.applyMatchesRegexCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.NOT_CONTAINS)) {
            return this.applyNotContainsCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.NOT_ENDS_WITH)) {
            return this.applyNotEndsWithCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.NOT_EQUALS)) {
            return this.applyNotEqualsCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.NOT_MATCHES_REGEX)) {
            return this.applyNotMatchesRegexCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.NOT_STARTS_WITH)) {
            return this.applyNotStartsWithCondition(whereBuilder, matchMode, searchText);
        } else if (condition.equals(StringCondition.STARTS_WITH)) {
            return this.applyStartsWithCondition(whereBuilder, matchMode, searchText);
        } else {
            throw new IllegalStateException("Unknown StringCondition: '" + condition.getClass().getName() + "'!");
        }
    }

    private FinalizableQueryBuilder applyStartsWithCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.startsWithIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.startsWith(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyNotStartsWithCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.notStartsWithIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.notStartsWith(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyNotEqualsCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.isNotEqualToIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.isNotEqualTo(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyNotEndsWithCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.notEndsWithIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.notEndsWith(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyNotContainsCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.notContainsIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.notContains(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyEqualsCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.isEqualToIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.isEqualTo(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyEndsWithCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.endsWithIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.endsWith(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyContainsCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.containsIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.contains(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }

    private FinalizableQueryBuilder applyMatchesRegexCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.matchesRegexIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.matchesRegex(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }


    private FinalizableQueryBuilder applyNotMatchesRegexCondition(final WhereBuilder whereBuilder, final TextMatchMode matchMode, final String searchText) {
        switch (matchMode) {
            case CASE_INSENSITIVE:
                return whereBuilder.notMatchesRegexIgnoreCase(searchText);
            case STRICT:
                return whereBuilder.notMatchesRegex(searchText);
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
    }


    private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
                                                   final LongSearchSpecification searchSpec) {
        NumberCondition condition = searchSpec.getCondition();
        long comparisonValue = searchSpec.getSearchValue();
        if (condition.equals(NumberCondition.EQUALS)) {
            return whereBuilder.isEqualTo(comparisonValue);
        } else if (condition.equals(NumberCondition.NOT_EQUALS)) {
            return whereBuilder.isNotEqualTo(comparisonValue);
        } else if (condition.equals(NumberCondition.LESS_EQUAL)) {
            return whereBuilder.isLessThanOrEqualTo(comparisonValue);
        } else if (condition.equals(NumberCondition.LESS_THAN)) {
            return whereBuilder.isLessThan(comparisonValue);
        } else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
            return whereBuilder.isGreaterThanOrEqualTo(comparisonValue);
        } else if (condition.equals(NumberCondition.GREATER_THAN)) {
            return whereBuilder.isGreaterThan(comparisonValue);
        } else {
            throw new IllegalStateException("Unknown NumberCondition: '" + condition.getClass().getName() + "'!");
        }
    }

    private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
                                                   final DoubleSearchSpecification searchSpec) {
        NumberCondition condition = searchSpec.getCondition();
        double comparisonValue = searchSpec.getSearchValue();
        double equalityTolerance = searchSpec.getEqualityTolerance();
        if (condition.equals(NumberCondition.EQUALS)) {
            return whereBuilder.isEqualTo(comparisonValue, equalityTolerance);
        } else if (condition.equals(NumberCondition.NOT_EQUALS)) {
            return whereBuilder.isNotEqualTo(comparisonValue, equalityTolerance);
        } else if (condition.equals(NumberCondition.LESS_EQUAL)) {
            return whereBuilder.isLessThanOrEqualTo(comparisonValue);
        } else if (condition.equals(NumberCondition.LESS_THAN)) {
            return whereBuilder.isLessThan(comparisonValue);
        } else if (condition.equals(NumberCondition.GREATER_EQUAL)) {
            return whereBuilder.isGreaterThanOrEqualTo(comparisonValue);
        } else if (condition.equals(NumberCondition.GREATER_THAN)) {
            return whereBuilder.isGreaterThan(comparisonValue);
        } else {
            throw new IllegalStateException("Unknown NumberCondition: '" + condition.getClass().getName() + "'!");
        }
    }

    private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
                                                   final ContainmentStringSearchSpecification searchSpec) {
        StringContainmentCondition condition = searchSpec.getCondition();
        TextMatchMode matchMode = searchSpec.getMatchMode();
        Set<String> searchValue = searchSpec.getSearchValue();
        if (condition.equals(StringContainmentCondition.WITHIN)) {
            switch (matchMode) {
                case STRICT:
                    return whereBuilder.inStrings(searchValue);
                case CASE_INSENSITIVE:
                    return whereBuilder.inStringsIgnoreCase(searchValue);
                default:
                    throw new UnknownEnumLiteralException(matchMode);
            }
        } else if (condition.equals(StringContainmentCondition.WITHOUT)) {
            switch (matchMode) {
                case STRICT:
                    return whereBuilder.notInStrings(searchValue);
                case CASE_INSENSITIVE:
                    return whereBuilder.notInStringsIgnoreCase(searchValue);
                default:
                    throw new UnknownEnumLiteralException(matchMode);
            }
        } else {
            throw new IllegalStateException("Unknown StringContainmentCondition: '" + condition.getClass().getName() + "'!");
        }
    }

    private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
                                                   final ContainmentLongSearchSpecification searchSpec) {
        LongContainmentCondition condition = searchSpec.getCondition();
        Set<Long> searchValue = searchSpec.getSearchValue();
        if (condition.equals(LongContainmentCondition.WITHIN)) {
            return whereBuilder.inLongs(searchValue);
        } else if (condition.equals(LongContainmentCondition.WITHOUT)) {
            return whereBuilder.notInLongs(searchValue);
        } else {
            throw new IllegalStateException("Unknown LongContainmentCondition: '" + condition.getClass().getName() + "'!");
        }
    }

    private FinalizableQueryBuilder applyCondition(final WhereBuilder whereBuilder,
                                                   final ContainmentDoubleSearchSpecification searchSpec) {
        DoubleContainmentCondition condition = searchSpec.getCondition();
        Set<Double> searchValue = searchSpec.getSearchValue();
        double equalityTolerance = searchSpec.getEqualityTolerance();
        if (condition.equals(DoubleContainmentCondition.WITHIN)) {
            return whereBuilder.inDoubles(searchValue, equalityTolerance);
        } else if (condition.equals(DoubleContainmentCondition.WITHOUT)) {
            return whereBuilder.notInDoubles(searchValue, equalityTolerance);
        } else {
            throw new IllegalStateException("Unknown DoubleContainmentCondition: '" + condition.getClass().getName() + "'!");
        }
    }

}
