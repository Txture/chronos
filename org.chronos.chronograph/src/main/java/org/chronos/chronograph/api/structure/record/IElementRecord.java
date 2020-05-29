package org.chronos.chronograph.api.structure.record;

import java.util.Set;

public interface IElementRecord extends Record {

    public String getId();

    public String getLabel();

    public Set<? extends IPropertyRecord> getProperties();

    public IPropertyRecord getProperty(String propertyKey);

}
