package org.chronos.chronodb.internal.impl.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.*;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.inmemory.InMemoryIndexManagerBackend;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocument;
import org.chronos.chronodb.internal.api.index.ChronoIndexDocumentModifications;
import org.chronos.chronodb.internal.api.index.DocumentAddition;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.index.cursor.IndexScanCursor;
import org.chronos.chronodb.internal.impl.index.diff.IndexValueDiff;
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils;
import org.chronos.common.autolock.AutoLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class DocumentBasedIndexManager extends AbstractIndexManager<ChronoDBInternal> {

    private InMemoryIndexManagerBackend backend;

    // =====================================================================================================================
    // CONSTRUCTOR
    // =====================================================================================================================

    public DocumentBasedIndexManager(final ChronoDBInternal owningDB) {
        super(owningDB);
        this.backend = new InMemoryIndexManagerBackend(owningDB);
        this.initializeIndicesFromDisk();
    }

    // =================================================================================================================
    // INDEX MANAGEMENT METHODS
    // =================================================================================================================

    @NotNull
    @Override
    public Set<SecondaryIndexImpl> loadIndicesFromPersistence() {
        return this.backend.loadIndexersFromPersistence();
    }

    @Override
    public void deleteIndexInternal(@NotNull final SecondaryIndexImpl index) {
        this.backend.deleteIndexContentsAndIndex(index);
    }

    @Override
    public void deleteAllIndicesInternal() {
        this.backend.deleteAllIndicesAndIndexers();
    }

    @Override
    public void saveIndexInternal(@NotNull final SecondaryIndexImpl index) {
        this.backend.persistIndex(index);
    }

    @Override
    public void rollback(@NotNull final SecondaryIndexImpl index, final long timestamp) {
        this.backend.rollback(Collections.singleton(index), timestamp);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void rollback(@NotNull final Set<SecondaryIndexImpl> indices, final long timestamp) {
        this.backend.rollback((Set) indices, timestamp);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void rollback(@NotNull final Set<SecondaryIndexImpl> indices, final long timestamp, @NotNull final Set<? extends QualifiedKey> keys) {
        this.backend.rollback((Set) indices, timestamp, (Set) keys);
    }

    @Override
    public void saveIndicesInternal(@NotNull final Set<SecondaryIndexImpl> indices) {
        this.backend.persistIndexers(indices);
    }

    // =================================================================================================================
    // INDEXING METHODS
    // =================================================================================================================

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void reindexAll(final boolean force) {
        try (AutoLock lock = this.getOwningDB().lockExclusive()) {
            Set<SecondaryIndex> indices;
            if (force) {
                indices = this.getOwningDB().getIndexManager().getIndices();
                // drop ALL index files (more efficient implementation than the override with 'dirtyIndices' parameter)
                this.backend.deleteAllIndexContents();
            } else {
                indices = this.getOwningDB().getIndexManager().getDirtyIndices();
                // delete all index data we have for those indices
                indices.forEach(this.backend::deleteIndexContents);
            }
            this.createIndexBaselines(indices);

            ListMultimap<String, SecondaryIndex> indicesByBranch = Multimaps.index(indices, SecondaryIndex::getBranch);
            Set<BranchInternal> branches = indicesByBranch.keySet().stream().map(b -> this.getOwningDB().getBranchManager().getBranch(b)).collect(Collectors.toSet());


            // then, iterate over the contents of the database
            Map<ChronoIdentifier, Pair<Object, Object>> identifierToValue = Maps.newHashMap();
            SerializationManager serializationManager = this.getOwningDB().getSerializationManager();
            for (BranchInternal branch : branches) {
                TemporalKeyValueStore tkvs = branch.getTemporalKeyValueStore();
                long now = tkvs.getNow();
                try (CloseableIterator<ChronoDBEntry> entries = tkvs.allEntriesIterator(0, now)) {
                    while (entries.hasNext()) {
                        ChronoDBEntry entry = entries.next();
                        ChronoIdentifier identifier = entry.getIdentifier();
                        byte[] value = entry.getValue();
                        Object deserializedValue = null;
                        if (value != null && value.length > 0) {
                            // only deserialize if the stored value is non-null
                            deserializedValue = serializationManager.deserialize(value);
                        }
                        ChronoDBTransaction historyTx = tkvs.tx(branch.getName(), identifier.getTimestamp() - 1);
                        Object historyValue = historyTx.get(identifier.getKeyspace(), identifier.getKey());
                        identifierToValue.put(identifier, Pair.of(historyValue, deserializedValue));
                    }
                }
            }
            this.index(identifierToValue, indices);
            // clear the query cache
            this.clearQueryCache();
            this.getOwningDB().getStatisticsManager().clearBranchHeadStatistics();
            for (SecondaryIndex index : indices) {
                ((SecondaryIndexImpl) index).setDirty(false);
            }
            this.saveIndicesInternal((Set) indices);
        }
    }

    private void createIndexBaselines(final Set<SecondaryIndex> indices) {
        if (indices.isEmpty()) {
            return;
        }
        Set<SecondaryIndex> unbasedIndices = indices.stream()
            // do not include inherited indices (for those we need no baseline, because the queries
            // will be redirected to the parent index anyways).
            .filter(idx -> idx.getParentIndexId() == null)
            // only consider the indices on non-master branches
            .filter(idx -> !idx.getBranch().equals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER))
            .collect(Collectors.toSet());

        if (unbasedIndices.isEmpty()) {
            return;
        }
        ListMultimap<String, SecondaryIndex> indicesByBranch = Multimaps.index(indices, SecondaryIndex::getBranch);
        for (Entry<String, Collection<SecondaryIndex>> branchToIndices : indicesByBranch.asMap().entrySet()) {
            String branch = branchToIndices.getKey();
            Collection<SecondaryIndex> branchIndices = branchToIndices.getValue();

            ListMultimap<Long, SecondaryIndex> unbasedIndexGroup = Multimaps.index(branchIndices, idx -> idx.getValidPeriod().getLowerBound());
            for (Entry<Long, Collection<SecondaryIndex>> entry : unbasedIndexGroup.asMap().entrySet()) {
                Long startTimestamp = entry.getKey();
                Collection<SecondaryIndex> indicesInGroup = entry.getValue();
                ChronoDBTransaction chronoDbTransaction = this.getOwningDB().tx(branch, startTimestamp);

                for (String keyspace : chronoDbTransaction.keyspaces()) {
                    Set<String> keySet = chronoDbTransaction.keySet(keyspace);
                    for (String key : keySet) {
                        ChronoIndexDocumentModifications modifications = ChronoIndexDocumentModifications.create();
                        Object value = chronoDbTransaction.get(keyspace, key);
                        if (value == null) {
                            continue; // safeguard, can't happen
                        }
                        for (SecondaryIndex index : indicesInGroup) {
                            Set<Comparable<?>> indexValues = index.getIndexedValuesForObject(value);
                            for (Object indexValue : indexValues) {
                                modifications.addDocumentCreation(DocumentAddition.create(
                                    ChronoIdentifier.create(branch, startTimestamp, keyspace, key), index, indexValue
                                ));
                            }
                        }
                        this.backend.applyModifications(modifications);
                    }
                }
            }
        }
    }


    @Override
    public void index(@NotNull final Map<ChronoIdentifier, ? extends Pair<Object, Object>> identifierToOldAndNewValue) {
        checkNotNull(identifierToOldAndNewValue,
            "Precondition violation - argument 'identifierToOldAndNewValue' must not be NULL!");
        Set<SecondaryIndex> indices = this.indexTree.getAllIndices().stream()
            .filter(idx -> !idx.getDirty()).collect(Collectors.toSet());

        this.index(identifierToOldAndNewValue, indices);
    }

    @SuppressWarnings("unchecked")
    public void index(@NotNull final Map<ChronoIdentifier, ? extends Pair<Object, Object>> identifierToOldAndNewValue, Set<SecondaryIndex> indices) {
        checkNotNull(identifierToOldAndNewValue,
            "Precondition violation - argument 'identifierToOldAndNewValue' must not be NULL!");
        checkNotNull(indices,
            "Precondition violation - argument 'indices' must not be NULL!");
        if (indices.isEmpty() || identifierToOldAndNewValue.isEmpty()) {
            return;
        }

        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            new IndexingProcess(indices).index((Map<ChronoIdentifier, Pair<Object, Object>>) identifierToOldAndNewValue);
        }

    }

    // =====================================================================================================================
    // QUERY METHODS
    // =====================================================================================================================

    @NotNull
    @Override
    public Set<String> performIndexQuery(
        final long timestamp,
        @NotNull final Branch branch,
        @NotNull final String keyspace,
        final SearchSpecification<?, ?> searchSpec
    ) {
        try (AutoLock lock = this.getOwningDB().lockNonExclusive()) {
            // check if we are dealing with a negated search specification that accepts empty values.
            if (searchSpec.getCondition().isNegated() && searchSpec.getCondition().acceptsEmptyValue()) {
                // the search spec is a negated condition that accepts the empty value.
                // To resolve this condition:
                // - Call keySet() on the target keyspace
                // - query the index with the non-negated condition
                // - subtract the matches from the keyset
                Set<String> keySet = this.getOwningDB().tx(branch.getName(), timestamp).keySet(keyspace);
                SearchSpecification<?, ?> nonNegatedSearch = searchSpec.negate();
                Collection<ChronoIndexDocument> documents = this.backend
                    .getMatchingDocuments(timestamp, branch, keyspace, nonNegatedSearch);
                // subtract the matches from the keyset
                for (ChronoIndexDocument document : documents) {
                    String key = document.getKey();
                    keySet.remove(key);
                }
                return Collections.unmodifiableSet(keySet);
            } else {
                Collection<ChronoIndexDocument> documents = this.backend
                    .getMatchingDocuments(timestamp, branch, keyspace, searchSpec);
                return Collections
                    .unmodifiableSet(documents.stream().map(ChronoIndexDocument::getKey).collect(Collectors.toSet()));
            }
        }
    }

    @NotNull
    @Override
    protected IndexScanCursor<?> createCursorInternal(
        @NotNull final Branch branch,
        final long timestamp,
        @NotNull final SecondaryIndex index,
        @NotNull final String keyspace,
        @NotNull final String indexName,
        @NotNull final Order order,
        @NotNull final TextCompare textCompare,
        @Nullable final Set<String> keys
    ) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        checkNotNull(textCompare, "Precondition violation - argument 'textCompare' must not be NULL!");
        return this.backend.createCursorOnIndex(
            branch,
            timestamp,
            index,
            keyspace,
            indexName,
            order,
            textCompare,
            keys
        );
    }

    // =====================================================================================================================
    // INNER CLASSES
    // =====================================================================================================================

    private class IndexingProcess {

        private final Set<SecondaryIndex> indices;
        private long currentTimestamp = -1L;
        private ChronoIndexDocumentModifications indexModifications;
        private Branch branch;

        IndexingProcess(Set<SecondaryIndex> indices) {
            this.indices = indices;
        }

        public void index(final Map<ChronoIdentifier, Pair<Object, Object>> identifierToValue) {
            checkNotNull(identifierToValue, "Precondition violation - argument 'identifierToValue' must not be NULL!");
            // build the indexer workload. The primary purpose is to sort the entries of the map in an order suitable
            // for processing.
            List<Pair<ChronoIdentifier, Pair<Object, Object>>> workload = IndexerWorkloadSorter.sort(identifierToValue);
            // get the iterator over the workload
            Iterator<Pair<ChronoIdentifier, Pair<Object, Object>>> iterator = workload.iterator();
            ListMultimap<String, SecondaryIndex> indicesByBranch = Multimaps.index(this.indices, SecondaryIndex::getBranch);
            // iterate over the workload
            while (iterator.hasNext()) {
                Pair<ChronoIdentifier, Pair<Object, Object>> entry = iterator.next();
                // unwrap the chrono identifier and the value to index associated with it
                ChronoIdentifier chronoIdentifier = entry.getKey();
                // check if we need to perform any periodic tasks
                this.checkCurrentTimestamp(chronoIdentifier.getTimestamp());
                this.checkCurrentBranch(chronoIdentifier.getBranchName());
                // index the single entry
                Pair<Object, Object> oldAndNewValue = identifierToValue.get(chronoIdentifier);
                Object oldValue = oldAndNewValue.getLeft();
                Object newValue = oldAndNewValue.getRight();
                List<SecondaryIndex> indices = indicesByBranch.get(chronoIdentifier.getBranchName());
                Set<SecondaryIndex> filteredIndices = indices.stream()
                    .filter(index -> !index.getValidPeriod().isBefore(chronoIdentifier.getTimestamp()))
                    .collect(Collectors.toSet());
                this.indexSingleEntry(chronoIdentifier, oldValue, newValue, filteredIndices);
            }
            // apply any remaining index modifications
            if (this.indexModifications.isEmpty() == false) {
                DocumentBasedIndexManager.this.backend.applyModifications(this.indexModifications);
            }
        }

        private void checkCurrentBranch(final String nextBranchName) {
            if (this.branch == null || this.branch.getName().equals(nextBranchName) == false) {
                // the branch is not the same as in the previous entry. Fetch the
                // branch metadata from the database.
                this.branch = DocumentBasedIndexManager.this.getOwningDB().getBranchManager().getBranch(nextBranchName);
            }
        }

        private void checkCurrentTimestamp(final long nextTimestamp) {
            if (this.currentTimestamp < 0 || this.currentTimestamp != nextTimestamp) {
                // the timestamp of the new work item is different from the one before. We need
                // to apply any index modifications (if any) and open a new modifications object
                if (this.indexModifications != null) {
                    DocumentBasedIndexManager.this.backend.applyModifications(this.indexModifications);
                }
                this.currentTimestamp = nextTimestamp;
                this.indexModifications = ChronoIndexDocumentModifications.create();
            }
        }

        private void indexSingleEntry(final ChronoIdentifier chronoIdentifier, final Object oldValue,
                                      final Object newValue, final Set<SecondaryIndex> secondaryIndices) {
            // in order to correctly treat the deletions, we need to query the index backend for
            // the currently active documents. We load these on-demand, because we don't need them in
            // the common case of indexing previously unseen (new) elements.
            Map<SecondaryIndex, SetMultimap<Object, ChronoIndexDocument>> oldDocuments = null;
            // calculate the diff
            IndexValueDiff diff = IndexingUtils.calculateDiff(secondaryIndices, oldValue, newValue);
            for (SecondaryIndex index : diff.getChangedIndices()) {
                Set<Object> addedValues = diff.getAdditions(index);
                Set<Object> removedValues = diff.getRemovals(index);
                // for each value we need to add, we create an index document based on the ChronoIdentifier.
                for (Object addedValue : addedValues) {
                    this.indexModifications.addDocumentAddition(chronoIdentifier, index, addedValue);
                }
                // iterate over the removed values and terminate the document validities
                for (Object removedValue : removedValues) {
                    if (oldDocuments == null) {
                        // make sure that the current index documents are available
                        oldDocuments = DocumentBasedIndexManager.this.backend
                            .getMatchingBranchLocalDocuments(chronoIdentifier);
                    }
                    SetMultimap<Object, ChronoIndexDocument> indexedValueToOldDoc = oldDocuments.get(index);
                    if (indexedValueToOldDoc == null) {
                        // There is no document for the old index value in our branch. This means that this indexed
                        // value was never touched in our branch. To "simulate" a valdity termination, we
                        // insert a new index document which is valid from the creation of our branch until
                        // our current timestamp.
                        ChronoIndexDocument document = new ChronoIndexDocumentImpl(index, this.branch.getName(),
                            chronoIdentifier.getKeyspace(), chronoIdentifier.getKey(), removedValue,
                            this.branch.getBranchingTimestamp());
                        document.setValidToTimestamp(this.currentTimestamp);
                        this.indexModifications.addDocumentAddition(document);
                    } else {
                        Set<ChronoIndexDocument> oldDocs = indexedValueToOldDoc.get(removedValue);
                        for (ChronoIndexDocument oldDocument : oldDocs) {
                            if (oldDocument.getValidToTimestamp() < Long.MAX_VALUE) {
                                // the document has already been closed. This can happen if a key-value pair has
                                // been inserted into the store, later deleted, and later re-inserted.
                                continue;
                            } else {
                                // the document belongs to our branch; terminate its validity
                                this.terminateDocumentValidityOrDeleteDocument(oldDocument, this.currentTimestamp);
                            }

                        }
                    }

                }
            }
        }

        private void terminateDocumentValidityOrDeleteDocument(final ChronoIndexDocument document,
                                                               final long timestamp) {
            checkNotNull(document, "Precondition violation - argument 'document' must not be NULL!");
            checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
            // check when the document was created
            if (document.getValidFromTimestamp() >= timestamp) {
                // the document was created at the same timestamp where we are going
                // to terminate it. That makes no sense, because the time ranges are
                // inclusive in the lower bound and exclusive in the upper bound.
                // Therefore, if lowerbound == upper bound, then the document needs
                // to be deleted instead. This situation can appear during incremental
                // commits.
                this.indexModifications.addDocumentDeletion(document);
            } else {
                // regularly terminate the validity of this document
                this.indexModifications.addDocumentValidityTermination(document, timestamp);
            }
        }
    }
}
