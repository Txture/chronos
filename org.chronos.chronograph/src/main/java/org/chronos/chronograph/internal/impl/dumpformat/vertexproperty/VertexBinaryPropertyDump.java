package org.chronos.chronograph.internal.impl.dumpformat.vertexproperty;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.chronos.chronograph.internal.impl.dumpformat.GraphDumpFormat;
import org.chronos.chronograph.internal.impl.dumpformat.property.AbstractPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.property.BinaryPropertyDump;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;

import com.google.common.collect.Maps;

public class VertexBinaryPropertyDump extends BinaryPropertyDump implements VertexPropertyDump {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	/**
	 * Used to hold the vertex property ID.
	 *
	 * <p>
	 * This field isn't used anymore and remains for backwards compatibility reasons.
	 * </p>
	 */
	@Deprecated
	@SuppressWarnings("unused")
	private String recordId;
	private Map<String, AbstractPropertyDump> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexBinaryPropertyDump() {
		// serialization constructor
	}

	public VertexBinaryPropertyDump(final IVertexPropertyRecord vpr) {
		super(vpr);
		this.properties = Maps.newHashMap();
		for (Entry<String, IPropertyRecord> entry : vpr.getProperties().entrySet()) {
			String key = entry.getKey();
			IPropertyRecord pRecord = entry.getValue();
			AbstractPropertyDump pDump = GraphDumpFormat.convertPropertyRecordToDumpFormat(pRecord);
			this.properties.put(key, pDump);
		}
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Map<String, AbstractPropertyDump> getProperties() {
		return Collections.unmodifiableMap(this.properties);
	}

}
