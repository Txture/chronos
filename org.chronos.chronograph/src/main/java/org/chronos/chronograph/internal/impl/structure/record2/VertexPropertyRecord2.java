package org.chronos.chronograph.internal.impl.structure.record2;

import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record3.VertexPropertyRecord3;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

/**
 * Represents the persistent state of a {@link VertexProperty}.
 *
 * <p>
 * This class has been <b>deprecated</b> because it still uses an explicit {@link #getId() ID} for each
 * vertex property, which is costly in practice and unnecessary. This class was replaced by {@link VertexPropertyRecord3}.
 * We keep this class around for migration and backwards compatibility purposes.
 * </p>
 *
 * @deprecated Use {@link VertexPropertyRecord3} instead. This class exists solely for migration and backwards compatibility purposes.
 *
 * @author martin.haeusler@txture io -- Deprecated this class in favour of {@link VertexPropertyRecord3}.
 * @author martin.haeusler@txture.io -- Initial contribution and API
 */
@Deprecated
@PersistentClass("kryo")
public final class VertexPropertyRecord2 extends PropertyRecord2 implements IVertexPropertyRecord {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String recordId;
	private Map<String, PropertyRecord2> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexPropertyRecord2() {
		// default constructor for serialization
	}

	public VertexPropertyRecord2(final String recordId, final String key, final Object value, final Iterator<Property<Object>> properties) {
		super(key, value);
		checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = recordId;
		if (properties.hasNext()) {
			this.properties = Maps.newHashMap();
			while (properties.hasNext()) {
				Property<?> property = properties.next();
				String pKey = property.key();
				Object pValue = property.value();
				PropertyRecord2 pRecord = new PropertyRecord2(pKey, pValue);
				this.properties.put(pKey, pRecord);
			}
		}
	}

	public VertexPropertyRecord2(final String recordId, final String key, final Object value, final Map<String, PropertyRecord2> properties) {
		super(key, value);
		checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = recordId;
		if (properties.isEmpty() == false) {
			this.properties = Maps.newHashMap();
			this.properties.putAll(properties);
		}
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
