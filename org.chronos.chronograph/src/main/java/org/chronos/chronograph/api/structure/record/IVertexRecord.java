package org.chronos.chronograph.api.structure.record;

import com.google.common.collect.SetMultimap;
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecordWithLabel;

import java.util.List;
import java.util.Set;

public interface IVertexRecord extends IElementRecord {

    public List<EdgeTargetRecordWithLabel> getIncomingEdges();

    public List<EdgeTargetRecordWithLabel> getIncomingEdges(String... labels);

    public SetMultimap<String, IEdgeTargetRecord> getIncomingEdgesByLabel();

    public List<EdgeTargetRecordWithLabel> getOutgoingEdges();

    public List<EdgeTargetRecordWithLabel> getOutgoingEdges(String... labels);

    public SetMultimap<String, IEdgeTargetRecord> getOutgoingEdgesByLabel();

    @Override
    public Set<IVertexPropertyRecord> getProperties();

    @Override
    public IVertexPropertyRecord getProperty(String propertyKey);

    public static IVertexRecordBuilder builder(){
        return new IVertexRecordBuilder();
    }

}
