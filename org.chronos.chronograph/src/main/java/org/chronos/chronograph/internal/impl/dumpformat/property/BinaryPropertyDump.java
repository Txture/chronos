package org.chronos.chronograph.internal.impl.dumpformat.property;

import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.common.serialization.KryoManager;

public class BinaryPropertyDump extends AbstractPropertyDump {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private byte[] value;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected BinaryPropertyDump() {
		// serialization constructor
	}

	public BinaryPropertyDump(final IPropertyRecord record) {
		super(record.getKey());
		this.value = KryoManager.serialize(record.getSerializationSafeValue());
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Object getValue() {
		if (this.value == null) {
			return null;
		}
		return KryoManager.deserialize(this.value);
	}

}
