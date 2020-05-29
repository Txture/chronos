package org.chronos.chronograph.internal.impl.structure.record;

import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record2.PropertyRecord2;
import org.chronos.common.annotation.PersistentClass;
import org.chronos.common.serialization.KryoManager;

/**
 *
 * @deprecated Replaced by {@link PropertyRecord2}. We keep this class to be able to read older graphs.
 */
@Deprecated
@PersistentClass("kryo")
public class PropertyRecord implements IPropertyRecord {

	@SuppressWarnings("unused")
	private String key;
	@SuppressWarnings("unused")
	private byte[] value;

	protected PropertyRecord() {
		// default constructor for serialization
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public Object getValue() {
		return KryoManager.deserialize(this.value);
	}

	@Override
	public Object getSerializationSafeValue() {
		return this.getValue();
	}

}
