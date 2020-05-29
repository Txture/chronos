package org.chronos.chronograph.internal.impl.dumpformat.converter;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.internal.impl.dumpformat.EdgeDump;

public class EdgeRecordConverter implements ChronoConverter<IEdgeRecord, EdgeDump> {

	@Override
	public EdgeDump writeToOutput(final IEdgeRecord record) {
		if (record == null) {
			return null;
		}
		return new EdgeDump(record);
	}

	@Override
	public IEdgeRecord readFromInput(final EdgeDump dump) {
		if (dump == null) {
			return null;
		}
		return dump.toRecord();
	}

}
