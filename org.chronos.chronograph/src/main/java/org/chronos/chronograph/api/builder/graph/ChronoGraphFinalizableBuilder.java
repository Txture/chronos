package org.chronos.chronograph.api.builder.graph;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.common.builder.ChronoBuilder;

/**
 * A builder for instances of {@link ChronoGraph}.
 *
 * <p>
 * When an instance of this interface is returned by the fluent builder API, then all information required for building
 * the database is complete, and {@link #build()} can be called to finalize the build process.
 *
 * <p>
 * Even though the {@link #build()} method becomes available at this stage, it is still possible to set properties
 * defined by the concrete implementations.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @param <SELF>
 *            The dynamic type of <code>this</code> to return for method chaining.
 */
public interface ChronoGraphFinalizableBuilder extends ChronoBuilder<ChronoGraphFinalizableBuilder> {

	/**
	 * Enables or disables the check for ID existence when adding a new graph element with a user-provided ID.
	 * 
	 * <p>
	 * For details, please refer to {@link ChronoGraphConfiguration#isCheckIdExistenceOnAddEnabled()}.
	 * 
	 * @param enableIdExistenceCheckOnAdd
	 *            Use <code>true</code> to enable the safety check, or <code>false</code> to disable it.
	 * 
	 * @return <code>this</code>, for method chaining.
	 */
	public ChronoGraphFinalizableBuilder withIdExistenceCheckOnAdd(boolean enableIdExistenceCheckOnAdd);

	/**
	 * Enables or disables automatic opening of new graph transactions on-demand.
	 * 
	 * <p>
	 * For details, please refer to {@link ChronoGraphConfiguration#isTransactionAutoOpenEnabled()}.
	 * 
	 * @param enableAutoStartTransactions
	 *            Set this to <code>true</code> if auto-start of transactions should be enabled (default), otherwise set
	 *            it to <code>false</code> to disable this feature.
	 * 
	 * @return <code>this</code>, for method chaining.
	 */
	public ChronoGraphFinalizableBuilder withTransactionAutoStart(boolean enableAutoStartTransactions);


	/**
	 * Builds the {@link ChronoGraph} instance, using the properties specified by the fluent API.
	 *
	 * <p>
	 * This method finalizes the build process. Afterwards, the builder should be discarded.
	 *
	 * @return The new {@link ChronoGraph} instance. Never <code>null</code>.
	 */
	public ChronoGraph build();

}