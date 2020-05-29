package org.chronos.chronograph.internal.impl.structure.record;

import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record2.VertexPropertyRecord2;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.Map;

/**
 * @deprecated Replaced by {@link VertexPropertyRecord2}. We keep this instance to be able to read older graphs.
 */
@Deprecated
@PersistentClass("kryo")
public final class VertexPropertyRecord extends PropertyRecord implements IVertexPropertyRecord {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	@SuppressWarnings("unused")
	private String recordId;
	@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
	private Map<String, PropertyRecord> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexPropertyRecord() {
		// default constructor for serialization
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public String getId() {
		return this.recordId;
	}

	@Override
	public Map<String, IPropertyRecord> getProperties() {
		if (this.properties == null) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(this.properties);
	}

}
