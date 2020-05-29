package org.chronos.chronograph.internal.impl.structure.record3;

import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record2.PropertyRecord2;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.Map;

/**
 * A {@link SimpleVertexPropertyRecord} is a {@link IVertexPropertyRecord} which has no {@link #getProperties()} meta-properties}.
 *
 * The reason this class exists is that meta-properties are rarely used in practice. This class
 * has no field to hold them (it just returns an empty map on {@link #getProperties()}), which makes
 * this class smaller on disk and in memory.
 *
 * @author martin.haeusler@txture.io -- Initial contribution and API
 */
@PersistentClass("kryo")
public class SimpleVertexPropertyRecord extends PropertyRecord2 implements IVertexPropertyRecord {

    protected SimpleVertexPropertyRecord(){
        // default constructor for (de-)serialization
    }

    public SimpleVertexPropertyRecord(final String key, final Object value){
        super(key, value);
    }

    @Override
    public Map<String, IPropertyRecord> getProperties() {
        // simple vertex property records have no meta-properties.
        return Collections.emptyMap();
    }
}
