package org.chronos.chronograph.internal.api.transaction;

import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;

public interface ChronoGraphTransactionInternal extends ChronoGraphTransaction {

    public ChronoEdge loadIncomingEdgeFromEdgeTargetRecord(ChronoVertexImpl targetVertex, String label,
                                                           IEdgeTargetRecord record);

    public ChronoEdge loadOutgoingEdgeFromEdgeTargetRecord(ChronoVertexImpl sourceVertex, String label,
                                                           IEdgeTargetRecord record);

    public IVertexRecord loadVertexRecord(String recordId);

    public IEdgeRecord loadEdgeRecord(final String recordId);

    public default void assertIsOpen() {
        if (this.isOpen()) {
            return;
        }
        // the TX has been closed. Try to find out if the graph itself was closed.
        Boolean graphClosed;
        try {
            ChronoGraph graph = this.getGraph();
            // threaded transaction graphs are bound to their transaction. If the transaction
            // is closed, the graph is closed. For this case, we're interested in the status
            // of the *original* graph on which the transaction graph operates.
            if (graph instanceof ChronoThreadedTransactionGraph) {
                graphClosed = ((ChronoThreadedTransactionGraph) graph).isOriginalGraphClosed();
            } else {
                graphClosed = graph.isClosed();
            }
        } catch (Exception e) {
            // we were unable to detect if the owning graph has been closed or not...
            graphClosed = null;
        }
        String graphDetailMessage;
        if (graphClosed == null) {
            graphDetailMessage = "";
        } else if (graphClosed == true) {
            graphDetailMessage = " The ChronoGraph instance has also been closed.";
        } else {
            graphDetailMessage = " The ChronoGraph instance is still open.";
        }

        throw new IllegalStateException(
            "This operation is bound to a Threaded Transaction, which was already closed. "
                + "Cannot continue to operate on this element." + graphDetailMessage);
    }
}
