package org.chronos.chronodb.internal.impl.engines.base;

import org.chronos.chronodb.api.ChangeSetEntry;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;

public class ThreadSafeChronoDBTransaction extends StandardChronoDBTransaction {

    public ThreadSafeChronoDBTransaction(final TemporalKeyValueStore tkvs, final long timestamp,
                                         final String branchIdentifier, final Configuration configuration) {
        super(tkvs, timestamp, branchIdentifier, configuration);
    }

    @Override
    protected synchronized void putInternal(final QualifiedKey key, final Object value, final PutOption[] options) {
        ChangeSetEntry entry = ChangeSetEntry.createChange(key, value, options);
        this.changeSet.put(key, entry);
    }

    @Override
    protected synchronized void removeInternal(final QualifiedKey key) {
        ChangeSetEntry entry = ChangeSetEntry.createDeletion(key);
        this.changeSet.put(key, entry);
    }

}
