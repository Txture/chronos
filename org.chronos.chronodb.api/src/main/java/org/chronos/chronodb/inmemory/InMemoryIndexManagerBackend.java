package org.chronos.chronodb.inmemory;

import com.google.common.collect.*;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.TextCompare;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.index.*;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.impl.engines.base.AbstractDocumentBasedIndexManagerBackend;
import org.chronos.chronodb.internal.impl.index.AbstractIndexManager;
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl;
import org.chronos.chronodb.internal.impl.index.cursor.BasicIndexScanCursor;
import org.chronos.chronodb.internal.impl.index.cursor.DeltaResolvingScanCursor;
import org.chronos.chronodb.internal.impl.index.cursor.IndexScanCursor;
import org.chronos.chronodb.internal.impl.index.cursor.RawIndexCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class InMemoryIndexManagerBackend extends AbstractDocumentBasedIndexManagerBackend {

    private static final Logger log = LoggerFactory.getLogger(InMemoryIndexManagerBackend.class);


    /**
     * Index -> Index Documents
     */
    protected final SetMultimap<SecondaryIndex, ChronoIndexDocument> indexToDocuments;

    /**
     * Index -> Keyspace Name -> Key -> Index Documents
     */
    protected final Map<SecondaryIndex, Map<String, SetMultimap<String, ChronoIndexDocument>>> documents;

    /**
     * Index name -> indexers
     */
    protected final Set<SecondaryIndexImpl> allIndices = Sets.newHashSet();

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public InMemoryIndexManagerBackend(final ChronoDBInternal owningDB) {
        super(owningDB);
        this.indexToDocuments = HashMultimap.create();
        this.documents = Maps.newHashMap();
    }

    // =================================================================================================================
    // INDEXER MANAGEMENT
    // =================================================================================================================

    public Set<SecondaryIndexImpl> loadIndexersFromPersistence() {
        return Collections.unmodifiableSet(this.allIndices);
    }

    public void persistIndexers(final Set<SecondaryIndexImpl> indices) {
        this.allIndices.clear();
        this.allIndices.addAll(indices);
    }

    @Override
    public void deleteAllIndexContents() {
        this.documents.clear();
        this.indexToDocuments.clear();
    }

    public void deleteAllIndicesAndIndexers() {
        this.allIndices.clear();
        this.documents.clear();
        this.indexToDocuments.clear();
    }

    @Override
    public void deleteIndexContents(final SecondaryIndex index) {
        this.documents.remove(index);
        this.indexToDocuments.removeAll(index);
    }

    public void deleteIndexContentsAndIndex(final SecondaryIndex index) {
        this.deleteIndexContents(index);
        this.allIndices.remove((SecondaryIndexImpl) index);
    }

    public void persistIndex(final SecondaryIndexImpl index) {
        this.allIndices.add(index);
    }

    // =================================================================================================================
    // INDEX DOCUMENT MANAGEMENT
    // =================================================================================================================

    @Override
    public void applyModifications(final ChronoIndexDocumentModifications indexModifications) {
        checkNotNull(indexModifications, "Precondition violation - argument 'indexModifications' must not be NULL!");
        if (indexModifications.isEmpty()) {
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("Applying index modifications: " + indexModifications);
        }
        for (DocumentValidityTermination termination : indexModifications.getDocumentValidityTerminations()) {
            ChronoIndexDocument document = termination.getDocument();
            long timestamp = termination.getTerminationTimestamp();
            this.terminateDocumentValidity(document, timestamp);
        }
        for (DocumentAddition creation : indexModifications.getDocumentCreations()) {
            ChronoIndexDocument document = creation.getDocumentToAdd();
            this.addDocument(document);
        }
        for (DocumentDeletion deletion : indexModifications.getDocumentDeletions()) {
            ChronoIndexDocument document = deletion.getDocumentToDelete();
            String branchName = document.getBranch();
            SecondaryIndex index = document.getIndex();
            if (!index.getBranch().equals(branchName)) {
                continue;
            }
            if (!index.getValidPeriod().contains(document.getValidFromTimestamp())) {
                continue;
            }
            // remove from index-name-to-documents map
            this.indexToDocuments.remove(index, document);
            // remove from general documents map
            Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKey = this.documents
                .get(index);
            if (keyspaceToKey == null) {
                continue;
            }
            SetMultimap<String, ChronoIndexDocument> keysToDocuments = keyspaceToKey.get(document.getKeyspace());
            if (keysToDocuments == null) {
                continue;
            }
            Set<ChronoIndexDocument> documents = keysToDocuments.get(document.getKey());
            if (documents == null) {
                continue;
            }
            documents.remove(document);
        }
    }

    @Override
    protected Set<ChronoIndexDocument> getDocumentsTouchedAtOrAfterTimestamp(final long timestamp,
                                                                             final Set<SecondaryIndex> indices) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        Set<ChronoIndexDocument> resultSet = Sets.newHashSet();
        if (indices != null && indices.isEmpty()) {
            // no indices are requested, so the result set is empty by definition.
            return resultSet;
        }
        for (ChronoIndexDocument document : this.indexToDocuments.values()) {
            if (indices != null && indices.contains(document.getIndex()) == false) {
                // the index of the document is not in the set of requested indices -> ignore the document
                continue;
            }
            if (document.getValidFromTimestamp() >= timestamp) {
                // the document was added at or after the timestamp in question
                resultSet.add(document);
            } else if (document.getValidToTimestamp() < Long.MAX_VALUE
                && document.getValidToTimestamp() >= timestamp) {
                // the document was modified at or after the timestamp in question
                resultSet.add(document);
            }
        }
        return resultSet;
    }

    // =================================================================================================================
    // INDEX QUERYING
    // =================================================================================================================

    @Override
    public Map<SecondaryIndex, SetMultimap<Object, ChronoIndexDocument>> getMatchingBranchLocalDocuments(
        final ChronoIdentifier chronoIdentifier) {
        checkNotNull(chronoIdentifier, "Precondition violation - argument 'chronoIdentifier' must not be NULL!");
        Map<SecondaryIndex, SetMultimap<Object, ChronoIndexDocument>> indexToIndexedValueToDocument = Maps.newHashMap();
        for (Entry<SecondaryIndex, Map<String, SetMultimap<String, ChronoIndexDocument>>> entry : this.documents.entrySet()) {
            SecondaryIndex index = entry.getKey();
            if (!index.getValidPeriod().contains(chronoIdentifier.getTimestamp())) {
                continue;
            }
            Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKey = entry.getValue();
            if (keyspaceToKey == null) {
                continue;
            }
            SetMultimap<String, ChronoIndexDocument> keyToDocument = keyspaceToKey.get(chronoIdentifier.getKeyspace());
            if (keyToDocument == null) {
                continue;
            }
            Set<ChronoIndexDocument> documents = keyToDocument.get(chronoIdentifier.getKey());
            if (documents == null) {
                continue;
            }
            for (ChronoIndexDocument document : documents) {
                Object indexedValue = document.getIndexedValue();
                SetMultimap<Object, ChronoIndexDocument> indexedValueToDocuments = indexToIndexedValueToDocument.get(index);
                if (indexedValueToDocuments == null) {
                    indexedValueToDocuments = HashMultimap.create();
                    indexToIndexedValueToDocument.put(index, indexedValueToDocuments);
                }
                indexedValueToDocuments.put(indexedValue, document);
            }
        }
        return indexToIndexedValueToDocument;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public IndexScanCursor<?> createCursorOnIndex(
        final Branch branch,
        final long timestamp,
        final SecondaryIndex index,
        final String keyspace,
        final String indexName,
        final Order order,
        final TextCompare textCompare,
        final Set<String> keys
    ) {
        checkArgument(index.getBranch().equals(branch.getName()),
            "Precondition violation - argument 'index' refers to a different branch than the given argument 'branch'!"
        );

        // TODO: this is GOD AWFUL. We really need to redo the inmemory backend...
        Set<ChronoIndexDocument> documents = this.indexToDocuments.get(index);
        List<IndexEntry> indexContents = Multimaps.index(
            documents, doc -> new IndexKey((Comparable<?>) doc.getIndexedValue(), doc.getKey())
        ).asMap().entrySet().stream().map(entry -> {
            IndexKey key = entry.getKey();
            List<Period> validPeriods = entry.getValue().stream()
                .map(ChronoIndexDocument::getValidPeriod)
                .sorted()
                .collect(Collectors.toList());
            return new IndexEntry(key, validPeriods);
        }).sorted(IndexEntry.createComparator(textCompare)).collect(Collectors.toList());
        if(order == Order.DESCENDING){
            indexContents = Lists.reverse(indexContents);
        }
        RawIndexCursor<?> rawCursor = new RawInMemoryIndexCursor(indexContents.iterator(), order);
        if (branch.getName().equals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)) {
            // on the master branch, we can use the simple scans as we don't have
            // to watch out for deltas.

            IndexScanCursor<?> cursor = new BasicIndexScanCursor(rawCursor, timestamp);
            if (keys != null) {
                cursor = cursor.filter(k -> keys.contains(k.getSecond()));
            }
            return cursor;
        }

        // create the cursor on the parent
        Branch parentBranch = branch.getOrigin();
        long parentTimestamp = Math.min(timestamp, branch.getBranchingTimestamp());

        AbstractIndexManager indexManager = (AbstractIndexManager) this.owningDB.getIndexManager();
        IndexScanCursor<?> parentCursor = indexManager.createCursor(parentTimestamp, parentBranch, keyspace, indexName, order, textCompare, keys);

        IndexScanCursor<?> scanCursor = new DeltaResolvingScanCursor(parentCursor, timestamp, rawCursor);
        if (keys != null) {
            scanCursor = scanCursor.filter(k -> keys.contains(k.getSecond()));
        }
        return scanCursor;
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    @Override
    protected Set<ChronoIndexDocument> getMatchingBranchLocalDocuments(
        final long timestamp,
        final String branchName,
        final String keyspace,
        final SearchSpecification<?, ?> searchSpec
    ) {
        checkArgument(timestamp >= 0,
            "Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
        SecondaryIndex index = searchSpec.getIndex();

        Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKeyToDoc = this.documents.get(index);
        if (keyspaceToKeyToDoc == null || keyspaceToKeyToDoc.isEmpty()) {
            return Collections.emptySet();
        }
        SetMultimap<String, ChronoIndexDocument> keyToDoc = keyspaceToKeyToDoc.get(keyspace);
        if (keyToDoc == null || keyToDoc.isEmpty()) {
            return Collections.emptySet();
        }
        Predicate<? super ChronoIndexDocument> filter = this.createMatchFilter(timestamp, searchSpec.toFilterPredicate());
        return Collections.unmodifiableSet(keyToDoc.values().stream().filter(filter).collect(Collectors.toSet()));
    }

    @Override
    protected Set<ChronoIndexDocument> getTerminatedBranchLocalDocuments(final long timestamp, final String branchName, final String keyspace,
                                                                         final SearchSpecification<?, ?> searchSpec) {
        checkArgument(timestamp >= 0,
            "Precondition violation - argument 'timestamp' must be >= 0 (value: " + timestamp + ")!");
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
        SecondaryIndex index = searchSpec.getIndex();

        Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToKeyToDoc = this.documents.get(index);
        if (keyspaceToKeyToDoc == null || keyspaceToKeyToDoc.isEmpty()) {
            return Collections.emptySet();
        }
        SetMultimap<String, ChronoIndexDocument> keyToDoc = keyspaceToKeyToDoc.get(keyspace);
        if (keyToDoc == null || keyToDoc.isEmpty()) {
            return Collections.emptySet();
        }
        Predicate<? super ChronoIndexDocument> filter = this.createDeletionFilter(timestamp, searchSpec.toFilterPredicate());
        return Collections.unmodifiableSet(keyToDoc.values().stream().filter(filter).collect(Collectors.toSet()));
    }

    private Predicate<? super ChronoIndexDocument> createMatchFilter(final long timestamp, final Predicate<Object> filterPredicate) {
        return (doc) -> {
            Object indexedValue = doc.getIndexedValue();
            boolean timeRangeOk = doc.getValidFromTimestamp() <= timestamp && timestamp < doc.getValidToTimestamp();
            if (timeRangeOk == false) {
                return false;
            }
            return filterPredicate.test(indexedValue);
        };
    }

    private Predicate<? super ChronoIndexDocument> createDeletionFilter(final long timestamp, final Predicate<Object> filterPredicate) {
        return (doc) -> {
            boolean timeRangeOk = doc.getValidToTimestamp() <= timestamp;
            if (timeRangeOk == false) {
                return false;
            }
            return filterPredicate.test(doc.getIndexedValue());
        };
    }

    protected void terminateDocumentValidity(final ChronoIndexDocument indexDocument, final long timestamp) {
        checkNotNull(indexDocument, "Precondition violation - argument 'indexDocument' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        // for in-memory, we only need to set the termination timestamp
        indexDocument.setValidToTimestamp(timestamp);
    }

    protected void addDocument(final ChronoIndexDocument document) {
        SecondaryIndex index = document.getIndex();
        this.indexToDocuments.put(index, document);
        String branch = document.getBranch();
        String keyspace = document.getKeyspace();
        String key = document.getKey();
        Map<String, SetMultimap<String, ChronoIndexDocument>> keyspaceToDocuments = this.documents.get(index);
        if (keyspaceToDocuments == null) {
            keyspaceToDocuments = Maps.newHashMap();
            this.documents.put(index, keyspaceToDocuments);
        }
        SetMultimap<String, ChronoIndexDocument> keyToDocuments = keyspaceToDocuments.get(keyspace);
        if (keyToDocuments == null) {
            keyToDocuments = HashMultimap.create();
            keyspaceToDocuments.put(keyspace, keyToDocuments);
        }
        keyToDocuments.put(key, document);
    }
}
