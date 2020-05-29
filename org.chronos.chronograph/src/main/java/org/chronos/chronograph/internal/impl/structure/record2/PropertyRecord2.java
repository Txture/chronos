package org.chronos.chronograph.internal.impl.structure.record2;

import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record2.valuerecords.PropertyRecordValue;
import org.chronos.common.annotation.PersistentClass;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecord2 implements IPropertyRecord {

	private String key;
	private PropertyRecordValue<?> value;

	protected PropertyRecord2() {
		// default constructor for serialization
	}

	public PropertyRecord2(final String key, final Object value) {
		checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		this.key = key;
		this.value = PropertyRecordValue.of(value);
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public Object getValue() {
		return this.value.getValue();
	}

	@Override
	public Object getSerializationSafeValue() {
		return this.value.getSerializationSafeValue();
	}

}
