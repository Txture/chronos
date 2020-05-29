package org.chronos.chronograph.internal.impl.structure.record;

import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;

import static com.google.common.base.Preconditions.*;

public class EdgeTargetRecordWithLabel {

    private final IEdgeTargetRecord record;
    private final String label;

    public EdgeTargetRecordWithLabel(IEdgeTargetRecord record, String label){
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        this.record = record;
        this.label = label;
    }

    public IEdgeTargetRecord getRecord() {
        return record;
    }

    public String getLabel() {
        return label;
    }
}
