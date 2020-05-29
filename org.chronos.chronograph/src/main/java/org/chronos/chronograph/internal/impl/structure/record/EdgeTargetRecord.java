package org.chronos.chronograph.internal.impl.structure.record;

import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.chronograph.internal.impl.structure.record2.EdgeTargetRecord2;
import org.chronos.common.annotation.PersistentClass;

/**
 * @deprecated Use {@link EdgeTargetRecord2} instead. We keep this class to be able to read older graph formats.
 */
@PersistentClass("kryo")
@Deprecated
public class EdgeTargetRecord implements IEdgeTargetRecord {

    @SuppressWarnings("unused")
    /** The string representation of the {@link ChronoVertexId} of the vertex at the "other end" of the edge. Unused because new instances are of type {@link EdgeTargetRecord2}. */
    private String otherEndVertexId;
    @SuppressWarnings("unused")
    /** The string representation of the {@link ChronoEdgeId} of the edge itself. Unused because new instances are of type {@link EdgeTargetRecord2}.*/
    private String edgeId;

    protected EdgeTargetRecord() {
        // default constructor for serialization
    }

    @Override
    public String getEdgeId() {
        return this.edgeId;
    }

    @Override
    public String getOtherEndVertexId() {
        return this.otherEndVertexId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.edgeId == null ? 0 : this.edgeId.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        EdgeTargetRecord other = (EdgeTargetRecord) obj;
        if (this.edgeId == null) {
            if (other.edgeId != null) {
                return false;
            }
        } else if (!this.edgeId.equals(other.edgeId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "EdgeTargetRecord[edgeId='" + this.edgeId + "', otherEndVertexId='" + this.otherEndVertexId + "']";
    }

}
