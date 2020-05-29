package org.chronos.chronograph.api.structure.record;

public interface IEdgeRecord extends IElementRecord {

    String getOutVertexId();

    String getInVertexId();

    public static IEdgeRecordBuilder builder(){
        return new IEdgeRecordBuilder();
    }

}
