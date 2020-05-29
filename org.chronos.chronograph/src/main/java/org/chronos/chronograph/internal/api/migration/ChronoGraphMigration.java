package org.chronos.chronograph.internal.api.migration;

import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpBinaryEntry;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpPlainEntry;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.common.serialization.KryoManager;
import org.chronos.common.version.ChronosVersion;

public interface ChronoGraphMigration {

    public static final Object ENTRY_UNCHANGED = new Object();
    public static final Object DROP_ENTRY = new Object();

    public ChronosVersion getFromVersion();

    public ChronosVersion getToVersion();

    public void execute(ChronoGraphInternal graph);

    public void execute(ChronoDBDumpMetadata dumpMetadata);

    public Object execute(ChronoIdentifier chronoIdentifier, Object value);

    public default <T> ChronoDBDumpEntry<T> execute(ChronoDBDumpEntry<T> dumpEntry) {
        if (dumpEntry instanceof ChronoDBDumpPlainEntry) {
            ChronoDBDumpPlainEntry plainEntry = (ChronoDBDumpPlainEntry) dumpEntry;
            ChronoIdentifier chronoIdentifier = plainEntry.getChronoIdentifier();
            Object value = plainEntry.getValue();
            Object newValue = this.execute(chronoIdentifier, value);
            if (newValue == ENTRY_UNCHANGED) {
                return dumpEntry;
            } else if (newValue == DROP_ENTRY) {
                return null;
            } else {
                plainEntry.setValue(newValue);
                return dumpEntry;
            }
        } else if (dumpEntry instanceof ChronoDBDumpBinaryEntry) {
            ChronoDBDumpBinaryEntry binaryEntry = (ChronoDBDumpBinaryEntry) dumpEntry;
            ChronoIdentifier chronoIdentifier = binaryEntry.getChronoIdentifier();
            byte[] binaryValue = binaryEntry.getValue();
            Object value;
            if (binaryValue == null || binaryValue.length <= 0) {
                value = null;
            } else {
                value = KryoManager.deserialize(binaryValue);
            }
            Object newValue = this.execute(chronoIdentifier, value);
            if (newValue == ENTRY_UNCHANGED) {
                return dumpEntry;
            } else if (newValue == DROP_ENTRY) {
                return null;
            } else {
                binaryEntry.setValue(KryoManager.serialize(newValue));
                return dumpEntry;
            }
        } else {
            throw new IllegalArgumentException("Encountered unknown subclass of " + ChronoDBDumpEntry.class.getName() + ": " + dumpEntry.getClass().getName());
        }
    }

}
