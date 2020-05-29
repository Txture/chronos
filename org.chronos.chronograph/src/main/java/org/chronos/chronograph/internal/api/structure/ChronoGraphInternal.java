package org.chronos.chronograph.internal.api.structure;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.transaction.trigger.ChronoGraphTriggerManagerInternal;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.version.ChronosVersion;

import java.util.Optional;

public interface ChronoGraphInternal extends ChronoGraph {

    /**
     * Returns the {@link ChronoDB} instance that acts as the backing store for this {@link ChronoGraph}.
     *
     * @return The backing ChronoDB instance. Never <code>null</code>.
     */
    public ChronoDB getBackingDB();

    /**
     * Returns the trigger manager for this graph instance.
     *
     * @return The trigger manager. Never <code>null</code>.
     */
    public ChronoGraphTriggerManagerInternal getTriggerManager();

    /**
     * Returns the version of ChronoGraph currently stored in the database.
     *
     * @return The version. Never <code>null</code>.
     */
    public Optional<ChronosVersion> getStoredChronoGraphVersion();

    /**
     * Sets the ChronoGraph version to store in the database.
     *
     * @param version The new version. Must not be <code>null</code>. Must be
     *                greater than or equal to the currently stored version.
     */
    public void setStoredChronoGraphVersion(ChronosVersion version);

    /**
     * Acquires the commit lock.
     *
     * <p>
     * Use this in conjunction with <code>try-with-resources</code> statements for easy locking.
     *
     * @return The auto-closable commit lock. Never <code>null</code>.
     */
    public AutoLock commitLock();

}
