package org.chronos.chronodb.inmemory.builder;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.builder.database.ChronoDBBackendBuilder;
import org.chronos.chronodb.api.builder.database.ChronoDBFinalizableBuilder;

/**
 * A builder for in-memory instances of {@link ChronoDB}.
 *
 * <p>
 * In-memory instance have no persistent representation; all changes to them are gone when the JVM terminates and/or the
 * garbage collector removes the ChronoDB instance from memory.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBInMemoryBuilder extends ChronoDBFinalizableBuilder<ChronoDBInMemoryBuilder>, ChronoDBBackendBuilder {

}
