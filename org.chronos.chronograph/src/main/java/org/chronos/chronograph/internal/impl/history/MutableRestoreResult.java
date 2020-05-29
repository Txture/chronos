package org.chronos.chronograph.internal.impl.history;

import com.google.common.collect.Sets;
import org.chronos.chronograph.api.history.RestoreResult;

import java.util.Collections;
import java.util.Set;

public class MutableRestoreResult implements RestoreResult {

    // =================================================================================================================
    // STATIC
    // =================================================================================================================

    private static final RestoreResult EMPTY = new MutableRestoreResult();

    public static RestoreResult empty(){
        return EMPTY;
    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Set<String> successfullyRestoredVertexIds = Sets.newHashSet();
    private final Set<String> successfullyRestoredEdgeIds = Sets.newHashSet();
    private final Set<String> failedEdgeIds = Sets.newHashSet();

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public MutableRestoreResult(){

    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public Set<String> getSuccessfullyRestoredVertexIds() {
        return Collections.unmodifiableSet(this.successfullyRestoredVertexIds);
    }

    @Override
    public Set<String> getSuccessfullyRestoredEdgeIds() {
        return Collections.unmodifiableSet(this.successfullyRestoredEdgeIds);
    }

    @Override
    public Set<String> getFailedEdgeIds() {
        return Collections.unmodifiableSet(this.failedEdgeIds);
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    public void markVertexAsSuccessfullyRestored(String vertexId){
        this.successfullyRestoredVertexIds.add(vertexId);
    }

    public void markEdgeAsSuccessfullyRestored(String edgeId){
        this.failedEdgeIds.remove(edgeId);
        this.successfullyRestoredEdgeIds.add(edgeId);
    }

    public void markEdgeAsFailed(String edgeId){
        this.successfullyRestoredEdgeIds.remove(edgeId);
        this.failedEdgeIds.add(edgeId);
    }

}
