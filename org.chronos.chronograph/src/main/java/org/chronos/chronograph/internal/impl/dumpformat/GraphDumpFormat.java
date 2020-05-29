package org.chronos.chronograph.internal.impl.dumpformat;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronodb.internal.impl.dump.ChronoDBDumpUtil;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.internal.impl.dumpformat.converter.EdgeRecordConverter;
import org.chronos.chronograph.internal.impl.dumpformat.converter.VertexRecordConverter;
import org.chronos.chronograph.internal.impl.dumpformat.property.AbstractPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.property.BinaryPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.property.PlainPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.vertexproperty.VertexBinaryPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.vertexproperty.VertexPlainPropertyDump;
import org.chronos.chronograph.internal.impl.dumpformat.vertexproperty.VertexPropertyDump;
import org.chronos.chronograph.internal.impl.structure.record.*;
import org.chronos.chronograph.internal.impl.structure.record2.EdgeRecord2;
import org.chronos.chronograph.internal.impl.structure.record2.VertexRecord2;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphDumpFormat {

	public static AbstractPropertyDump convertPropertyRecordToDumpFormat(final IPropertyRecord record) {
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		Object value = record.getSerializationSafeValue();
		if (ChronoDBDumpUtil.isWellKnownObject(value)) {
			// it's a well-known type -> use plain text format
			return new PlainPropertyDump(record);
		}
		ChronoConverter<?, ?> converter = ChronoDBDumpUtil.getAnnotatedConverter(value);
		if (converter == null) {
			// no converter given -> use binary
			return new BinaryPropertyDump(record);
		} else {
			// use the converter for a plain text format
			return new PlainPropertyDump(record, converter);
		}
	}

	public static VertexPropertyDump convertVertexPropertyRecordToDumpFormat(final IVertexPropertyRecord record) {
		checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
		Object value = record.getSerializationSafeValue();
		if (ChronoDBDumpUtil.isWellKnownObject(value)) {
			// it's a well-known type -> use plain text format
			return new VertexPlainPropertyDump(record);
		}
		ChronoConverter<?, ?> converter = ChronoDBDumpUtil.getAnnotatedConverter(value);
		if (converter == null) {
			// no converter given -> use binary
			return new VertexBinaryPropertyDump(record);
		} else {
			// use the converter for a plain text format
			return new VertexPlainPropertyDump(record, converter);
		}
	}

	public static void registerGraphAliases(final DumpOptions options) {
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		options.enable(DumpOption.aliasHint(EdgeDump.class, "cEdge"));
		options.enable(DumpOption.aliasHint(VertexDump.class, "cVertex"));
		options.enable(DumpOption.aliasHint(PlainPropertyDump.class, "cPropertyPlain"));
		options.enable(DumpOption.aliasHint(BinaryPropertyDump.class, "cPropertyBinary"));
		options.enable(DumpOption.aliasHint(VertexPlainPropertyDump.class, "cVertexPropertyPlain"));
		options.enable(DumpOption.aliasHint(VertexBinaryPropertyDump.class, "cVertexPropertyBinary"));
		options.enable(DumpOption.aliasHint(EdgeTargetDump.class, "cEdgeTarget"));
	}

	public static void registerDefaultConvertersForReading(final DumpOptions options) {
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		options.enable(DumpOption.defaultConverter(VertexDump.class, new VertexRecordConverter()));
		options.enable(DumpOption.defaultConverter(EdgeDump.class, new EdgeRecordConverter()));
	}

	public static void registerDefaultConvertersForWriting(final DumpOptions options) {
		checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
		options.enable(DumpOption.defaultConverter(VertexRecord.class, new VertexRecordConverter()));
		options.enable(DumpOption.defaultConverter(VertexRecord2.class, new VertexRecordConverter()));
		options.enable(DumpOption.defaultConverter(EdgeRecord.class, new EdgeRecordConverter()));
		options.enable(DumpOption.defaultConverter(EdgeRecord2.class, new EdgeRecordConverter()));
	}
}
