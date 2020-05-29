package org.chronos.chronograph.internal.impl.structure.record3;

import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record2.PropertyRecord2;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

/**
 * Represents the persistent state of a {@link VertexProperty}.
 *
 * <p>
 * If a {@link VertexProperty} has no {@linkplain VertexProperty#properties(String...) meta-properties}, then
 * {@link SimpleVertexPropertyRecord} should be used instead of this class.
 * </p>
 *
 * @author martin.haeusler@txture.io -- Initial contribution and API.
 */
@PersistentClass("kryo")
public class VertexPropertyRecord3 extends PropertyRecord2 implements IVertexPropertyRecord {

    // =====================================================================================================================
    // FIELDS
    // =====================================================================================================================

    private Map<String, PropertyRecord2> properties;

    // =====================================================================================================================
    // CONSTRUCTORS
    // =====================================================================================================================

    protected VertexPropertyRecord3() {
        // default constructor for serialization
    }

    public VertexPropertyRecord3(final String key, final Object value, final Iterator<Property<Object>> properties) {
        super(key, value);
        checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
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

    public VertexPropertyRecord3(final String key, final Object value, final Map<String, PropertyRecord2> properties) {
        super(key, value);
        checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
        if (properties.isEmpty() == false) {
            this.properties = Maps.newHashMap();
            this.properties.putAll(properties);
        }
    }

    // =====================================================================================================================
    // PUBLIC API
    // =====================================================================================================================

    @Override
    public Map<String, IPropertyRecord> getProperties() {
        if (this.properties == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(this.properties);
    }

}
