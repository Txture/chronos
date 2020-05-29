package org.chronos.chronograph.internal.impl.structure.record2;

import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.common.annotation.PersistentClass;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class EdgeTargetRecord2 implements IEdgeTargetRecord {

	/** The string representation of the {@link ChronoVertexId} of the vertex at the "other end" of the edge. */
	private String otherEndVertexId;
	/** The string representation of the {@link ChronoEdgeId} of the edge itself. */
	private String edgeId;

	protected EdgeTargetRecord2() {
		// default constructor for serialization
	}

	public EdgeTargetRecord2(final String edgeId, final String otherEndVertexId) {
		checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
		checkNotNull(otherEndVertexId, "Precondition violation - argument 'otherEndVertexId' must not be NULL!");
		this.edgeId = edgeId;
		this.otherEndVertexId = otherEndVertexId;
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
		EdgeTargetRecord2 other = (EdgeTargetRecord2) obj;
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
