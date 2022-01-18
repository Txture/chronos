package org.chronos.chronograph.internal.api.index;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronograph.api.index.ChronoGraphIndex;

import java.util.Set;

/**
 * This class is the internal representation of {@link ChronoGraphIndex}, offering additional methods to be used by internal API only.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphIndexInternal extends ChronoGraphIndex {

    /**
     * Returns the index key used in the backing {@link ChronoDB} index.
     *
     * @return The backend index key. Never <code>null</code>.
     */
    public String getBackendIndexKey();

    /**
     * Returns the indexing options applied to the backing {@link ChronoDB} index.
     *
     * @return The backend indexing options. May be empty, but never <code>null</code>.
     */
    public Set<IndexingOption> getIndexingOptions();

}
