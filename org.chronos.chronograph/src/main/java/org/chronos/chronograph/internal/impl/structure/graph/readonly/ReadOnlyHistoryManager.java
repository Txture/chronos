package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronograph.api.history.ChronoGraphHistoryManager;
import org.chronos.chronograph.api.history.RestoreResult;

import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyHistoryManager implements ChronoGraphHistoryManager {

    private final ChronoGraphHistoryManager manager;

    public ReadOnlyHistoryManager(ChronoGraphHistoryManager manager){
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }


    @Override
    public RestoreResult restoreGraphElementsAsOf(final long timestamp, final Set<String> vertexIds, final Set<String> edgeIds) {
        throw new UnsupportedOperationException("This operation is not supported in a read-only graph!");
    }

    @Override
    public RestoreResult restoreGraphStateAsOf(final long timestamp) {
        throw new UnsupportedOperationException("This operation is not supported in a read-only graph!");
    }
}
