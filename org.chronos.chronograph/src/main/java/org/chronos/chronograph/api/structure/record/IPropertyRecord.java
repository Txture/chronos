package org.chronos.chronograph.api.structure.record;

public interface IPropertyRecord extends Record {

    public String getKey();

    public Object getValue();

    public Object getSerializationSafeValue();
}
