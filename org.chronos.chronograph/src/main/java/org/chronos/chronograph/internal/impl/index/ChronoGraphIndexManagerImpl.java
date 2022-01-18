package org.chronos.chronograph.internal.impl.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.*;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.builder.query.WhereBuilder;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.query.*;
import org.chronos.chronodb.internal.api.query.searchspec.*;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
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
    private final Branch branch;

    private final ReadWriteLock lock;

    public ChronoGraphIndexManagerImpl(final ChronoDB chronoDB, final String branchName) {
        checkNotNull(chronoDB, "Precondition violation - argument 'chronoDB' must not be NULL!");
        this.chronoDB = chronoDB;
        this.branch = this.chronoDB.getBranchManager().getBranch(branchName);
        this.lock = new ReentrantReadWriteLock(true);
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
    public Set<ChronoGraphIndex> getIndexedPropertiesAtTimestamp(final Class<? extends Element> clazz, long timestamp) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch, timestamp).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .filter(idx -> Objects.equals(idx.getIndexedElementClass(), clazz))
            .collect(Collectors.toSet())
        );
    }

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesAtAnyPointInTime(final Class<? extends Element> clazz) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .filter(idx -> Objects.equals(idx.getIndexedElementClass(), clazz))
            .collect(Collectors.toSet())
        );
    }

    @Override
    public Set<ChronoGraphIndex> getAllIndicesAtAnyPointInTime() {
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet())
        );
    }

    @Override
    public Set<ChronoGraphIndex> getAllIndicesAtTimestamp(final long timestamp) {
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch, timestamp).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet())
        );
    }

    @Override
    public ChronoGraphIndex getVertexIndexAtTimestamp(final String indexedPropertyName, final long timestamp) {
        checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch, timestamp).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .filter(idx -> Vertex.class.equals(idx.getIndexedElementClass()))
            .filter(idx -> idx.getValidPeriod().contains(timestamp))
            .filter(idx -> idx.getIndexedProperty().equals(indexedPropertyName))
            .findFirst().orElse(null)
        );
    }

    @Override
    public Set<ChronoGraphIndex> getVertexIndicesAtAnyPointInTime(final String indexedPropertyName) {
        checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .filter(idx -> Vertex.class.equals(idx.getIndexedElementClass()))
            .filter(idx -> idx.getIndexedProperty().equals(indexedPropertyName))
            .collect(Collectors.toSet())
        );
    }

    @Override
    public ChronoGraphIndex getEdgeIndexAtTimestamp(final String indexedPropertyName, final long timestamp) {
        checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch, timestamp).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .filter(idx -> Edge.class.equals(idx.getIndexedElementClass()))
            .filter(idx -> idx.getIndexedProperty().equals(indexedPropertyName))
            .filter(idx -> idx.getValidPeriod().contains(timestamp))
            .findFirst().orElse(null)
        );
    }

    @Override
    public Set<ChronoGraphIndex> getEdgeIndicesAtAnyPointInTime(final String indexedPropertyName) {
        checkNotNull(indexedPropertyName, "Precondition violation - argument 'indexedPropertyName' must not be NULL!");
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .filter(idx -> Edge.class.equals(idx.getIndexedElementClass()))
            .filter(idx -> idx.getIndexedProperty().equals(indexedPropertyName))
            .collect(Collectors.toSet())
        );
    }

    @Override
    public boolean isReindexingRequired() {
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().isReindexingRequired());
    }

    @Override
    public Set<ChronoGraphIndex> getDirtyIndicesAtAnyPointInTime() {
        return this.performNonExclusive(() -> this.getChronoDBIndexManager().getIndices(this.branch).stream()
            .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
            .filter(Objects::nonNull)
            .filter(ChronoGraphIndex3::isDirty)
            .collect(Collectors.toSet())
        );
    }

    // =====================================================================================================================
    // INDEX MANIPULATION
    // =====================================================================================================================

    @Override
    public void reindexAll(boolean force) {
        this.performExclusive(() -> this.getChronoDBIndexManager().reindexAll(force));
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index) {
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        this.performExclusive(() -> {
            IndexManager indexManager = this.getChronoDBIndexManager();
            SecondaryIndex chronoDbIndex = indexManager.getIndexById(index.getId());
            if (chronoDbIndex != null) {
                indexManager.deleteIndex(chronoDbIndex);
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
            Set<SecondaryIndex> indicesToDelete = this.getAllIndicesAtAnyPointInTime().stream()
                .map(graphIndex -> indexManager.getIndexById(graphIndex.getId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
            indexManager.deleteIndices(indicesToDelete);
        });
    }

    @Override
    public ChronoGraphIndex terminateIndex(final ChronoGraphIndex index, final long timestamp) {
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        Preconditions.checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.performExclusive(()-> {
            IndexManager indexManager = this.getChronoDBIndexManager();
            SecondaryIndex chronoDBIndex = indexManager.getIndexById(index.getId());
            ChronoGraphIndex existingIndex = Optional.ofNullable(chronoDBIndex)
                .map(ChronoGraphIndex3::createFromChronoDBIndexOrNull)
                .orElse(null);
            if(existingIndex == null){
                throw new IllegalArgumentException("The given index '" + index + "' does not exist anymore and cannot be terminated!");
            }
            if(existingIndex.getValidPeriod().getUpperBound() != Long.MAX_VALUE) {
                throw new IllegalStateException("The index '" + index + "' has already been terminated. It cannot be terminated it again!");
            }
            SecondaryIndex updatedIndex = indexManager.setIndexEndDate(chronoDBIndex, timestamp);
            return ChronoGraphIndex3.createFromChronoDBIndexOrNull(updatedIndex);
        });
    }

    // =====================================================================================================================
    // INTERNAL API :: MODIFICATION
    // =====================================================================================================================

    @Override
    public ChronoGraphIndex addIndex(
        final Class<? extends Element> elementType,
        final IndexType indexType,
        final String propertyName,
        final long startTimestamp,
        final long endTimestamp,
        final Set<IndexingOption> indexingOptions
    ) {
        checkNotNull(elementType, "Precondition violation - argument 'elementType' must not be NULL!");
        checkNotNull(indexType, "Precondition violation - argument 'indexType' must not be NULL!");
        checkNotNull(propertyName, "Precondition violation - argument 'propertyName' must not be NULL!");
        checkArgument(!propertyName.isEmpty(), "Precondition violation - argument 'propertyName' must not be empty!");
        checkArgument(startTimestamp <= System.currentTimeMillis(), "Precondition violation - argument 'startTimestamp' is in the future!");
        checkArgument(startTimestamp >= 0, "Precondition violation - argument 'startTimestamp' must not be negative!");
        checkArgument(endTimestamp > startTimestamp, "Precondition violation - argument 'endTimestamp' must be greater than 'startTimestamp'!");
        return this.performExclusive(() -> {
            String indexName;
            Indexer<?> indexer;
            if (Vertex.class.equals(elementType)) {
                indexName = ChronoGraphConstants.INDEX_PREFIX_VERTEX + propertyName;
                switch (indexType) {
                    case STRING:
                        indexer = new VertexRecordStringIndexer2(propertyName);
                        break;
                    case LONG:
                        indexer = new VertexRecordLongIndexer2(propertyName);
                        break;
                    case DOUBLE:
                        indexer = new VertexRecordDoubleIndexer2(propertyName);
                        break;
                    default:
                        throw new UnknownEnumLiteralException(indexType);
                }
            } else if (Edge.class.equals(elementType)) {
                indexName = ChronoGraphConstants.INDEX_PREFIX_EDGE + propertyName;
                switch (indexType) {
                    case STRING:
                        indexer = new EdgeRecordStringIndexer2(propertyName);
                        break;
                    case LONG:
                        indexer = new EdgeRecordLongIndexer2(propertyName);
                        break;
                    case DOUBLE:
                        indexer = new EdgeRecordDoubleIndexer2(propertyName);
                        break;
                    default:
                        throw new UnknownEnumLiteralException(indexType);
                }
            } else {
                throw new IllegalArgumentException("The elementType '" + elementType.getName() + "' is unknown! Only Vertex and Edge are allowed!");
            }
            SecondaryIndex dbIndex = this.chronoDB.getIndexManager().createIndex()
                .withName(indexName)
                .withIndexer(indexer)
                .onBranch(this.branch)
                .fromTimestamp(startTimestamp)
                .toTimestamp(endTimestamp)
                .withOptions(indexingOptions)
                .build();
            return ChronoGraphIndex3.createFromChronoDBIndexOrNull(dbIndex);
        });
    }

    // =====================================================================================================================
    // INTERNAL API :: SEARCH
    // =====================================================================================================================

    @Override
    public Iterator<String> findVertexIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?, ?>> searchSpecifications) {
        checkNotNull(
            searchSpecifications,
            "Precondition violation - argument 'searchSpecifications' must not be NULL!"
        );
        return this.findElementsByIndexedProperties(tx, Vertex.class, ChronoGraphConstants.KEYSPACE_VERTEX,
            searchSpecifications
        );
    }

    @Override
    public Iterator<String> findEdgeIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?, ?>> searchSpecifications) {
        checkNotNull(
            searchSpecifications,
            "Precondition violation - argument 'searchSpecifications' must not be NULL!"
        );
        return this.findElementsByIndexedProperties(tx, Edge.class, ChronoGraphConstants.KEYSPACE_EDGE,
            searchSpecifications
        );
    }

    private Iterator<String> findElementsByIndexedProperties(final ChronoGraphTransaction tx, final Class<? extends Element> clazz,
                                                             final String keyspace, final Set<SearchSpecification<?, ?>> searchSpecifications) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkNotNull(keyspace, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(
            searchSpecifications,
            "Precondition violation - argument 'searchSpecifications' must not be NULL!"
        );
        // we need to make sure that all given properties are indeed indexed
        checkArgument(
            searchSpecifications.isEmpty() == false,
            "Precondition violation - need at least one search specification to search for!"
        );
        Set<String> properties = searchSpecifications.stream().map(search -> search.getIndex().getName()).collect(Collectors.toSet());
        this.assertAllPropertiesAreIndexed(clazz, properties, tx.getTimestamp());
        // build a map from 'backend property key' to 'search specifications'
        SetMultimap<String, SearchSpecification<?, ?>> backendPropertyKeyToSearchSpecs = HashMultimap.create();
        Set<ChronoGraphIndex> graphIndices = this.getIndexedPropertiesAtTimestamp(clazz, tx.getTimestamp());
        for (SearchSpecification<?, ?> searchSpec : searchSpecifications) {
            ChronoGraphIndex index = graphIndices.stream()
                .filter(idx -> idx.getId().equals(searchSpec.getIndex().getId()))
                .findAny()
                .orElseThrow(() ->
                    new IllegalStateException("Unable to find graph index for ID '" + searchSpec.getIndex().getId() + "'!")
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
            throw new RuntimeException("Exception occurred while performing exclusive task: " + e, e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private <T> T performNonExclusive(final Callable<T> c) {
        this.lock.readLock().lock();
        try {
            return c.call();
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred while performing exclusive task: " + e, e);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    private void assertAllPropertiesAreIndexed(final Class<? extends Element> clazz, final Set<String> propertyNames, long timestamp) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkNotNull(propertyNames, "Precondition violation - argument 'propertyNames' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        if (Vertex.class.isAssignableFrom(clazz)) {
            Set<String> indexedVertexPropertyNames = this.getIndexedVertexPropertyNamesAtTimestamp(timestamp);
            Set<String> unindexedProperties = Sets.newHashSet(propertyNames);
            unindexedProperties.removeAll(indexedVertexPropertyNames);
            if (unindexedProperties.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Some of the given properties are not indexed on vertices on branch '"
                        + this.branch.getName() + "' at timestamp " + timestamp + ": "
                        + unindexedProperties
                );
            }
        } else if (Edge.class.isAssignableFrom(clazz)) {
            Set<String> indexedEdgePropertyNames = this.getIndexedEdgePropertyNamesAtTimestamp(timestamp);
            Set<String> unindexedProperties = Sets.newHashSet(propertyNames);
            unindexedProperties.removeAll(indexedEdgePropertyNames);
            if (unindexedProperties.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Some of the given properties are not indexed on edges on branch '"
                        + this.branch.getName() + "' at timestamp " + timestamp + ": "
                        + unindexedProperties
                );
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
