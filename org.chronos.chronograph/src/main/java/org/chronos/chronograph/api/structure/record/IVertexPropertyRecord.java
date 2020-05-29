package org.chronos.chronograph.api.structure.record;

import java.util.Map;

public interface IVertexPropertyRecord extends IPropertyRecord {

    public Map<String, IPropertyRecord> getProperties();

}
