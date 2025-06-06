package org.chronos.chronograph.internal.impl.transaction.threaded;

import com.google.common.collect.Maps;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.Order;
import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.history.ChronoGraphHistoryManager;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.maintenance.ChronoGraphMaintenanceManager;
import org.chronos.chronograph.api.schema.ChronoGraphSchemaManager;
import org.chronos.chronograph.api.statistics.ChronoGraphStatisticsManager;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoGraphVariables;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoGraphVariablesImpl;
import org.chronos.chronograph.internal.impl.transaction.trigger.ChronoGraphTriggerManagerInternal;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.version.ChronosVersion;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.*;

public class ChronoThreadedTransactionGraph implements ChronoGraphInternal {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ChronoGraphInternal originalGraph;
    private final ChronoGraphVariablesImpl variables;

    private final ChronoGraphTransactionManager txManager;
    private final Map<String, ChronoGraphIndexManager> branchNameToIndexManager;

    private boolean isClosed;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    public ChronoThreadedTransactionGraph(final ChronoGraphInternal originalGraph, final String branchName) {
        this(originalGraph, branchName, null);
    }

    public ChronoThreadedTransactionGraph(final ChronoGraphInternal originalGraph, final String branchName,
                                          final long timestamp) {
        this(originalGraph, branchName, Long.valueOf(timestamp));
    }

    public ChronoThreadedTransactionGraph(final ChronoGraphInternal originalGraph, final String branchName,
                                          final Long timestamp) {
        checkNotNull(originalGraph, "Precondition violation - argument 'originalGraph' must not be NULL!");
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        if (timestamp != null) {
            checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        }
        this.originalGraph = originalGraph;
        this.isClosed = false;
        // initialize the graph transaction which is bound to this graph
        ChronoDB db = this.originalGraph.getBackingDB();
        ChronoDBTransaction backendTransaction = null;
        if (timestamp == null) {
            // no timestamp given, use head revision
            backendTransaction = db.tx(branchName);
        } else {
            // timestamp given, use the given revision
            backendTransaction = db.tx(branchName, timestamp);
        }
        ThreadedChronoGraphTransaction graphTx = new ThreadedChronoGraphTransaction(this, backendTransaction);
        // build the "pseudo transaction manager" that only returns the just created graph transaction
        this.txManager = new ThreadedChronoGraphTransactionManager(this, graphTx);
        this.branchNameToIndexManager = Maps.newHashMap();
        this.variables = new ChronoGraphVariablesImpl(this);
    }

    // =================================================================================================================
    // GRAPH CLOSING
    // =================================================================================================================

    @Override
    public void close() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;
        // close the singleton transaction
        if (this.tx().isOpen()) {
            this.tx().rollback();
        }
    }

    @Override
    public boolean isClosed() {
        return this.isClosed || this.originalGraph.isClosed();
    }

    public boolean isOriginalGraphClosed(){
        return this.originalGraph.isClosed();
    }

    // =====================================================================================================================
    // VERTEX & EDGE HANDLING
    // =====================================================================================================================

    @Override
    public Vertex addVertex(final Object... keyValues) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().addVertex(keyValues);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().edges(edgeIds);
    }

    // =====================================================================================================================
    // COMPUTATION API
    // =====================================================================================================================

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    // =====================================================================================================================
    // TRANSACTION API
    // =====================================================================================================================

    @Override
    public ChronoGraphTransactionManager tx() {
        return this.txManager;
    }

    @Override
    public long getNow() {
        ChronoGraphTransaction transaction = this.tx().getCurrentTransaction();
        if (transaction.isOpen() == false) {
            throw new IllegalStateException("This threaded transaction was already closed!");
        }
        return this.getBackingDB().getBranchManager().getMasterBranch().getNow();
    }

    @Override
    public long getNow(final String branchName) {
        ChronoGraphTransaction transaction = this.tx().getCurrentTransaction();
        if (transaction.isOpen() == false) {
            throw new IllegalStateException("This threaded transaction was already closed!");
        }
        return this.getBackingDB().getBranchManager().getBranch(branchName).getNow();
    }

    // =====================================================================================================================
    // INDEXING
    // =====================================================================================================================

    @Override
    public ChronoGraphIndexManager getIndexManagerOnMaster() {
        return this.getIndexManagerOnBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
    }

    @Override
    public ChronoGraphIndexManager getIndexManagerOnBranch(final String branchName) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        if (this.getBackingDB().getBranchManager().existsBranch(branchName) == false) {
            throw new IllegalArgumentException("There is no branch named '" + branchName + "'!");
        }
        // try to retrieve a cached copy of the manager
        ChronoGraphIndexManager indexManager = this.branchNameToIndexManager.get(branchName);
        if (indexManager == null) {
            // manager not present in our cache; build it and add it to the cache
            indexManager = new ThreadedChronoGraphIndexManager(this, branchName);
            this.branchNameToIndexManager.put(branchName, indexManager);
        }
        return indexManager;
    }

    // =====================================================================================================================
    // BRANCHING
    // =====================================================================================================================

    @Override
    public ChronoGraphBranchManager getBranchManager() {
        return this.originalGraph.getBranchManager();
    }

    // =================================================================================================================
    // TRIGGERS
    // =================================================================================================================

    @Override
    public ChronoGraphTriggerManagerInternal getTriggerManager() {
        return this.originalGraph.getTriggerManager();
    }

    @Override
    public Optional<ChronosVersion> getStoredChronoGraphVersion() {
        return this.originalGraph.getStoredChronoGraphVersion();
    }

    @Override
    public void setStoredChronoGraphVersion(final ChronosVersion version) {
        this.originalGraph.setStoredChronoGraphVersion(version);
    }

    // =================================================================================================================
    // SCHEMA VALIDATION
    // =================================================================================================================

    @Override
    public ChronoGraphSchemaManager getSchemaManager() {
        return this.originalGraph.getSchemaManager();
    }

    // =================================================================================================================
    // MAINTENANCE
    // =================================================================================================================

    @Override
    public ChronoGraphMaintenanceManager getMaintenanceManager() {
        return this.originalGraph.getMaintenanceManager();
    }

    // =================================================================================================================
    // STATISTICS
    // =================================================================================================================

    @Override
    public ChronoGraphStatisticsManager getStatisticsManager() {
        return this.originalGraph.getStatisticsManager();
    }

    // =================================================================================================================
    // HISTORY
    // =================================================================================================================

    @Override
    public ChronoGraphHistoryManager getHistoryManager() {
        return this.originalGraph.getHistoryManager();
    }

    // =====================================================================================================================
    // VARIABLES & CONFIGURATION
    // =====================================================================================================================

    @Override
    public ChronoGraphVariables variables() {
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return this.originalGraph.configuration();
    }

    @Override
    public ChronoGraphConfiguration getChronoGraphConfiguration() {
        return this.originalGraph.getChronoGraphConfiguration();
    }

    // =====================================================================================================================
    // TEMPORAL ACTIONS
    // =====================================================================================================================

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getVertexHistory(vertexId);
    }

    @Override
    public Iterator<Long> getVertexHistory(final Object vertexId, final long lowerBound, final long upperBound, final Order order) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getVertexHistory(vertexId, lowerBound, upperBound, order);
    }

    @Override
    public Iterator<Long> getEdgeHistory(final Object edgeId, final long lowerBound, final long upperBound, final Order order) {
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getEdgeHistory(edgeId, lowerBound, upperBound, order);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfVertex(vertex);
    }

    @Override
    public long getLastModificationTimestampOfVertex(final Object vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfVertex(vertexId);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfEdge(edge);
    }

    @Override
    public long getLastModificationTimestampOfEdge(final Object edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        this.tx().readWrite();
        return this.tx().getCurrentTransaction().getLastModificationTimestampOfEdge(edgeId);
    }

    @Override
    public Iterator<Pair<Long, String>> getVertexModificationsBetween(final long timestampLowerBound,
                                                                      final long timestampUpperBound) {
        checkArgument(timestampLowerBound >= 0,
            "Precondition violation - argument 'timestampLowerBound' must not be negative!");
        checkArgument(timestampUpperBound >= 0,
            "Precondition violation - argument 'timestampUpperBound' must not be negative!");
        checkArgument(timestampLowerBound <= timestampUpperBound,
            "Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
        this.tx().readWrite();
        checkArgument(timestampLowerBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
        checkArgument(timestampUpperBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
        return this.tx().getCurrentTransaction().getVertexModificationsBetween(timestampLowerBound,
            timestampUpperBound);
    }

    @Override
    public Iterator<Pair<Long, String>> getEdgeModificationsBetween(final long timestampLowerBound,
                                                                    final long timestampUpperBound) {
        checkArgument(timestampLowerBound >= 0,
            "Precondition violation - argument 'timestampLowerBound' must not be negative!");
        checkArgument(timestampUpperBound >= 0,
            "Precondition violation - argument 'timestampUpperBound' must not be negative!");
        checkArgument(timestampLowerBound <= timestampUpperBound,
            "Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
        this.tx().readWrite();
        checkArgument(timestampLowerBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampLowerBound' must not exceed the transaction timestamp!");
        checkArgument(timestampUpperBound <= this.tx().getCurrentTransaction().getTimestamp(),
            "Precondition violation - argument 'timestampUpperBound' must not exceed the transaction timestamp!");
        return this.tx().getCurrentTransaction().getEdgeModificationsBetween(timestampLowerBound, timestampUpperBound);
    }

    @Override
    public Object getCommitMetadata(final String branch, final long timestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(timestamp <= this.getNow(branch),
            "Precondition violation - argument 'timestamp' must not be larger than the latest commit timestamp!");
        return this.originalGraph.getCommitMetadata(branch, timestamp);
    }

    @Override
    public Iterator<Long> getCommitTimestampsBetween(final String branch, final long from, final long to,
                                                     final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.originalGraph.getCommitTimestampsBetween(branch, from, to, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from, final long to,
                                                                  final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.originalGraph.getCommitMetadataBetween(branch, from, to, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Long> getCommitTimestampsPaged(final String branch, final long minTimestamp,
                                                   final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.originalGraph.getCommitTimestampsPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final String branch, final long minTimestamp,
                                                                final long maxTimestamp, final int pageSize, final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        return this.originalGraph.getCommitMetadataPaged(branch, minTimestamp, maxTimestamp, pageSize, pageIndex, order, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAround(final String branch, final long timestamp,
                                                             final int count, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        return this.originalGraph.getCommitMetadataAround(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataBefore(final String branch, final long timestamp,
                                                             final int count, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        return this.originalGraph.getCommitMetadataBefore(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAfter(final String branch, final long timestamp,
                                                            final int count, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        return this.originalGraph.getCommitMetadataAfter(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsAround(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        return this.originalGraph.getCommitTimestampsAround(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsBefore(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        return this.originalGraph.getCommitTimestampsBefore(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public List<Long> getCommitTimestampsAfter(final String branch, final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        return this.originalGraph.getCommitTimestampsAfter(branch, timestamp, count, includeSystemInternalCommits);
    }

    @Override
    public int countCommitTimestampsBetween(final String branch, final long from, final long to, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        return this.originalGraph.countCommitTimestampsBetween(branch, from, to, includeSystemInternalCommits);
    }

    @Override
    public int countCommitTimestamps(final String branch, final boolean includeSystemInternalCommits) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        return this.originalGraph.countCommitTimestamps(branch, includeSystemInternalCommits);
    }

    @Override
    public Iterator<String> getChangedVerticesAtCommit(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.originalGraph.getChangedVerticesAtCommit(branch, commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedEdgesAtCommit(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.originalGraph.getChangedEdgesAtCommit(branch, commitTimestamp);
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final long commitTimestamp) {
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.originalGraph.getChangedGraphVariablesAtCommit(commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final long commitTimestamp) {
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.originalGraph.getChangedGraphVariablesAtCommitInDefaultKeyspace(commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final long commitTimestamp, final String keyspace) {
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        return this.originalGraph.getChangedGraphVariablesAtCommitInKeyspace(commitTimestamp, keyspace);
    }

    @Override
    public Iterator<Pair<String, String>> getChangedGraphVariablesAtCommit(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.originalGraph.getChangedGraphVariablesAtCommit(branch, commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInDefaultKeyspace(final String branch, final long commitTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        return this.originalGraph.getChangedGraphVariablesAtCommitInDefaultKeyspace(branch, commitTimestamp);
    }

    @Override
    public Iterator<String> getChangedGraphVariablesAtCommitInKeyspace(final String branch, final long commitTimestamp, final String keyspace) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        return this.originalGraph.getChangedGraphVariablesAtCommitInKeyspace(branch, commitTimestamp, keyspace);
    }

    // =====================================================================================================================
    // SERIALIZATION & DESERIALIZATION (GraphSon, Gyro, ...)
    // =====================================================================================================================

    @Override
    @SuppressWarnings({"rawtypes"})
    public <I extends Io> I io(final Builder<I> builder) {
        return this.originalGraph.io(builder);
    }

    // =====================================================================================================================
    // DUMP OPERATIONS
    // =====================================================================================================================

    @Override
    public void writeDump(final File dumpFile, final DumpOption... dumpOptions) {
        throw new UnsupportedOperationException("createDump(...) is not permitted on threaded transaction graphs. "
            + "Call it on the original graph instead.");
    }

    @Override
    public void readDump(final File dumpFile, final DumpOption... options) {
        throw new UnsupportedOperationException("readDump(...) is not permitted on threaded transaction graphs. "
            + "Call it on the original graph instance.");
    }

    // =====================================================================================================================
    // STRING REPRESENTATION
    // =====================================================================================================================

    @Override
    public String toString() {
        // according to Tinkerpop specification...
        return StringFactory.graphString(this, "");
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    public ChronoDB getBackingDB() {
        return this.originalGraph.getBackingDB();
    }

    @Override
    public AutoLock commitLock() {
        return this.originalGraph.commitLock();
    }

    public ChronoGraph getOriginalGraph() {
        return this.originalGraph;
    }

    // =====================================================================================================================
    // FEATURES DECLARATION
    // =====================================================================================================================

    @Override
    public Features features() {
        return this.originalGraph.features();
    }

}
