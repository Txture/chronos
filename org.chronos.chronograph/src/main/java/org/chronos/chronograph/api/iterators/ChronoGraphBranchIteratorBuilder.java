package org.chronos.chronograph.api.iterators;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.chronos.chronograph.api.iterators.callbacks.TimestampChangeCallback;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;

public interface ChronoGraphBranchIteratorBuilder extends ChronoGraphIteratorBuilder {

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.overAllCommitTimestampsDescendingBefore(timestamp, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescendingBefore(timestamp, Integer.MAX_VALUE, callback);
    }


    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp, int limit) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        return this.overAllCommitTimestampsDescendingBefore(timestamp, limit, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp, int limit, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescendingBefore(timestamp, limit, t -> true, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp, Predicate<Long> filter) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overAllCommitTimestampsDescendingBefore(timestamp, Integer.MAX_VALUE, filter);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescendingBefore(timestamp, Integer.MAX_VALUE, filter, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp, int limit, Predicate<Long> filter) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overAllCommitTimestampsDescendingBefore(timestamp, limit, filter, TimestampChangeCallback.IGNORE);
    }


    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingBefore(long timestamp, int limit, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overTimestamps(branch -> {
            Iterator<Long> timestamps = this.getGraph().getCommitTimestampsBefore(branch, timestamp, limit).iterator();
            return Iterators.filter(timestamps, t -> filter.test(t));
        }, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.overAllCommitTimestampsDescendingAfter(timestamp, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescendingAfter(timestamp, Integer.MAX_VALUE, callback);
    }


    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp, int limit) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        return this.overAllCommitTimestampsDescendingAfter(timestamp, limit, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp, int limit, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescendingAfter(timestamp, limit, t -> true, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp, Predicate<Long> filter) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overAllCommitTimestampsDescendingAfter(timestamp, Integer.MAX_VALUE, filter);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescendingAfter(timestamp, Integer.MAX_VALUE, filter, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp, int limit, Predicate<Long> filter) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overAllCommitTimestampsDescendingAfter(timestamp, limit, filter, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescendingAfter(long timestamp, int limit, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overTimestamps(branch -> {
            Iterator<Long> timestamps = this.getGraph().getCommitTimestampsAfter(branch, timestamp, limit).iterator();
            return Iterators.filter(timestamps, t -> filter.test(t));
        }, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescending() {
        return this.overAllCommitTimestampsDescending(TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescending(TimestampChangeCallback callback) {
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescending(Integer.MAX_VALUE, (timestamp -> true), callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescending(Predicate<Long> filter) {
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overAllCommitTimestampsDescending(Integer.MAX_VALUE, filter, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescending(Predicate<Long> filter, TimestampChangeCallback callback) {
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overAllCommitTimestampsDescending(Integer.MAX_VALUE, filter, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overAllCommitTimestampsDescending(int limit, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overTimestamps(branch -> {
            Iterator<Long> timestamps = this.getGraph().getCommitTimestampsAfter(branch, 0L, limit).iterator();
            return Iterators.filter(timestamps, t -> filter.test(t));
        }, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.overHistoryOfVertex(vertexId, Integer.MAX_VALUE);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId, Predicate<Long> filter) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overHistoryOfVertex(vertexId, Integer.MAX_VALUE, filter);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId, TimestampChangeCallback callback) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overHistoryOfVertex(vertexId, Integer.MAX_VALUE, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId, int limit) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        return this.overHistoryOfVertex(vertexId, limit, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overHistoryOfVertex(vertexId, Integer.MAX_VALUE, filter, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId, int limit, TimestampChangeCallback callback) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overHistoryOfVertex(vertexId, limit, t -> true, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId, int limit, Predicate<Long> filter) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overHistoryOfVertex(vertexId, limit, filter, TimestampChangeCallback.IGNORE);
    }


    public default ChronoGraphTimestampIteratorBuilder overHistoryOfVertex(String vertexId, int limit, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overTimestamps(branch -> {
            ChronoGraph graph = this.getGraph();
            if (graph instanceof ChronoThreadedTransactionGraph) {
                graph = ((ChronoThreadedTransactionGraph) graph).getOriginalGraph();
            }
            List<Long> vertexHistory;
            ChronoGraph txGraph = graph.tx().createThreadedTx(branch);
            try {
                vertexHistory = Lists.newArrayList(txGraph.getVertexHistory(vertexId));
            } finally {
                txGraph.tx().rollback();
            }
            Iterator<Long> filtered = Iterators.filter(vertexHistory.iterator(), filter::test);
            return Iterators.limit(filtered, limit);
        }, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.overHistoryOfEdge(edgeId, Integer.MAX_VALUE);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId, Predicate<Long> filter) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overHistoryOfEdge(edgeId, Integer.MAX_VALUE, filter);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId, TimestampChangeCallback callback) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overHistoryOfEdge(edgeId, Integer.MAX_VALUE, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId, int limit) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        return this.overHistoryOfEdge(edgeId, limit, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overHistoryOfEdge(edgeId, Integer.MAX_VALUE, filter, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId, int limit, TimestampChangeCallback callback) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overHistoryOfEdge(edgeId, limit, t -> true, callback);
    }

    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId, int limit, Predicate<Long> filter) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        return this.overHistoryOfEdge(edgeId, limit, filter, TimestampChangeCallback.IGNORE);
    }


    public default ChronoGraphTimestampIteratorBuilder overHistoryOfEdge(String edgeId, int limit, Predicate<Long> filter, TimestampChangeCallback callback) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        checkArgument(limit > 0, "Precondition violation - argument 'limit' must be greater than zero!");
        checkNotNull(filter, "Precondition violation - argument 'filter' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overTimestamps(branch -> {
            ChronoGraph graph = this.getGraph();
            if (graph instanceof ChronoThreadedTransactionGraph) {
                graph = ((ChronoThreadedTransactionGraph) graph).getOriginalGraph();
            }
            List<Long> edgeHistory;
            ChronoGraph txGraph = graph.tx().createThreadedTx(branch);
            try {
                edgeHistory = Lists.newArrayList(txGraph.getEdgeHistory(edgeId));
            } finally {
                txGraph.tx().rollback();
            }
            Iterator<Long> filtered = Iterators.filter(edgeHistory.iterator(), filter::test);
            return Iterators.limit(filtered, limit);
        }, callback);
    }


    public default ChronoGraphTimestampIteratorBuilder atHead() {
        return this.overTimestamps((branch -> Iterators.singletonIterator(this.getGraph().getNow(branch))), TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder atTimestamp(long timestamp) {
        return this.overTimestamps((branch -> Iterators.singletonIterator(timestamp)), TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overTimestamps(Iterable<Long> timestamps) {
        checkNotNull(timestamps, "Precondition violation - argument 'timestamps' must not be NULL!");
        return this.overTimestamps(timestamps, TimestampChangeCallback.IGNORE);
    }

    public default ChronoGraphTimestampIteratorBuilder overTimestamps(Iterable<Long> timestamps, TimestampChangeCallback callback) {
        checkNotNull(timestamps, "Precondition violation - argument 'timestamps' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        return this.overTimestamps((branch -> timestamps.iterator()), callback);
    }

    public ChronoGraphTimestampIteratorBuilder overTimestamps(Function<String, Iterator<Long>> branchToTimestampsFunction, TimestampChangeCallback callback);

}
