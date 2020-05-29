package org.chronos.chronograph.internal.impl.dumpformat.converter;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.internal.impl.dumpformat.VertexDump;

public class VertexRecordConverter implements ChronoConverter<IVertexRecord, VertexDump> {

	@Override
	public VertexDump writeToOutput(final IVertexRecord record) {
		if (record == null) {
			return null;
		}
		return new VertexDump(record);
	}

	@Override
	public IVertexRecord readFromInput(final VertexDump dump) {
		if (dump == null) {
			return null;
		}
		return dump.toRecord();
	}

}
