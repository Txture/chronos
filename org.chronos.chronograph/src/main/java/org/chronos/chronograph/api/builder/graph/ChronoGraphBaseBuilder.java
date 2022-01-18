package org.chronos.chronograph.api.builder.graph;

import org.apache.commons.configuration2.Configuration;
import org.chronos.chronodb.api.builder.database.ChronoDBBackendBuilder;
import org.chronos.chronodb.api.builder.database.ChronoDBFinalizableBuilder;
import org.chronos.chronodb.inmemory.builder.ChronoDBInMemoryBuilder;
import org.chronos.chronograph.api.ChronoGraphFactory;
import org.chronos.chronograph.api.structure.ChronoGraph;

import java.io.File;
import java.util.Properties;
import java.util.function.Function;

/**
 * This class acts as the first step in the fluent ChronoGraph builder API.
 *
 * <p>
 * You can get access to an instance of this class via {@link ChronoGraphFactory#create()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphBaseBuilder {

	/**
	 * A generic way for creating a ChronoGraph instance.
	 *
	 * <p>
	 * Most users will not need to use this method directly. Instead, use e.g. {@link #exodusGraph(File)}, {@link #inMemoryGraph(Function)} or {@link #fromProperties(Properties)},
	 * which are essentially shortcuts for this method.
	 * </p>
	 *
	 * @param builder The ChronoDB-Builder to use for store configuration. Must not be <code>null</code>.
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphFinalizableBuilder graphOnChronoDB(ChronoDBFinalizableBuilder<?> builder);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain Properties properties} file.
	 *
	 * @param file The properties file to read the graph configuration from. Must not be <code>null</code>, must refer to an existing properties file.
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphFinalizableBuilder fromPropertiesFile(File file);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain Properties properties} file.
	 *
	 * @param filePath
	 *            The path to the properties file to read. Must not be <code>null</code>. Must refer to an existing file.
	 *
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphFinalizableBuilder fromPropertiesFile(String filePath);

	/**
	 * Creates a {@link ChronoGraph} based on an Apache Commons {@link Configuration} object.
	 *
	 * @param configuration
	 *            The configuration to use for the new graph. Must not be <code>null</code>.
	 *
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphFinalizableBuilder fromConfiguration(Configuration configuration);

	/**
	 * Creates a {@link ChronoGraph} based on a {@linkplain Properties properties} object.
	 *
	 * @param properties The properties object to read the settings from. Must not be <code>null</code>.
	 * @return The builder for the Graph, for method chaining. Never <code>null</code>.
	 */
	public ChronoGraphFinalizableBuilder fromProperties(Properties properties);

	/**
	 * Creates a basic in-memory ChronoGraph in its default configuration.
	 *
	 * <p>
	 * The in-memory graph is shipped together with the ChronoGraph API; no additional artifacts are necessary.
	 * </p>
	 *
	 * <p>
	 * While the in-memory backend is subject to the same suite of tests as all other backends, it is primarily
	 * intended as a stand-in for testing purposes.
	 * </p>
	 *
	 * @return The graph builder for further customization on graph-level. If you want customization on key-value-store level, please use {@link #inMemoryGraph(Function)} instead.
	 * @see #inMemoryGraph(Function)
	 */
	public ChronoGraphFinalizableBuilder inMemoryGraph();

	/**
	 * Creates an in-memory ChronoGraph and allows for customization of its configuration via the argument function.
	 *
	 * <p>
	 * The in-memory graph is shipped together with the ChronoGraph API; no additional artifacts are necessary.
	 * </p>
	 *
	 * <p>
	 * While the in-memory backend is subject to the same suite of tests as all other backends, it is primarily
	 * intended as a stand-in for testing purposes.
	 * </p>
	 *
	 * @param configureStore Configures the backing key-value-store. Please note that some settings which are mandatory for ChronoGraph will be silently overwritten.
	 *                       Important: do <b>not</b> call {@link ChronoDBFinalizableBuilder#build()} on the builder, ChronoGraph will do this internally.
	 * @return The graph builder for further customization on graph-level.
	 */
	public ChronoGraphFinalizableBuilder inMemoryGraph(Function<ChronoDBInMemoryBuilder, ChronoDBFinalizableBuilder<ChronoDBInMemoryBuilder>> configureStore);

	/**
	 * Creates a ChronoGraph based on the Exodus backend (the default production backend for ChronoGraph as of version 1.0.0) in its default configuration.
	 *
	 * <p>
	 * Please note that in order to use this backend, you need to have the <code>org.chronos.chronodb.exodus</code> artifact on your classpath.
	 * </p>
	 *
	 * @param directoryPath The path to the directory where you wish your graph data to be stored. Must not be <code>null</code>. Must point to a valid directory.
	 * @return The graph builder for further customization on graph-level.
	 * @see #exodusGraph(String, Function)
	 */
	public ChronoGraphFinalizableBuilder exodusGraph(String directoryPath);

	/**
	 * Creates a ChronoGraph based on the Exodus backend (the default production backend for ChronoGraph as of version 1.0.0) in its default configuration.
	 *
	 * <p>
	 * Please note that in order to use this backend, you need to have the <code>org.chronos.chronodb.exodus</code> artifact on your classpath.
	 * </p>
	 *
	 * @param directory The directory where you wish your graph data to be stored. Must not be <code>null</code>. Must point to a valid directory.
	 * @return The graph builder for further customization on graph-level.
	 * @see #exodusGraph(File, Function)
	 */
	public ChronoGraphFinalizableBuilder exodusGraph(File directory);

	/**
	 * Creates a ChronoGraph based on the Exodus backend (the default production backend for ChronoGraph as of version 1.0.0).
	 *
	 * <p>
	 * Please note that in order to use this backend, you need to have the <code>org.chronos.chronodb.exodus</code> artifact on your classpath.
	 * </p>
	 *
	 * @param directoryPath  The path to the directory where you wish your graph data to be stored. Must not be <code>null</code>. Must point to a valid directory.
	 * @param configureStore Configures the backing key-value-store. Please note that some settings which are mandatory for ChronoGraph will be silently overwritten.
	 *                       Important: do <b>not</b> call {@link ChronoDBFinalizableBuilder#build()} on the builder, ChronoGraph will do this internally.
	 * @return The graph builder for further customization on graph-level.
	 */
	public ChronoGraphFinalizableBuilder exodusGraph(String directoryPath, Function<ChronoDBFinalizableBuilder<?>, ChronoDBFinalizableBuilder<?>> configureStore);

	/**
	 * Creates a ChronoGraph based on the Exodus backend (the default production backend for ChronoGraph as of version 1.0.0).
	 *
	 * <p>
	 * Please note that in order to use this backend, you need to have the <code>org.chronos.chronodb.exodus</code> artifact on your classpath.
	 * </p>
	 *
	 * @param directory      The directory where you wish your graph data to be stored. Must not be <code>null</code>. Must point to a valid directory.
	 * @param configureStore Configures the backing key-value-store. Please note that some settings which are mandatory for ChronoGraph will be silently overwritten.
	 *                       Important: do <b>not</b> call {@link ChronoDBFinalizableBuilder#build()} on the builder, ChronoGraph will do this internally.
	 * @return The graph builder for further customization on graph-level.
	 * @see #exodusGraph(File, Function)
	 */
	public ChronoGraphFinalizableBuilder exodusGraph(File directory, Function<ChronoDBFinalizableBuilder<?>, ChronoDBFinalizableBuilder<?>> configureStore);

	/**
	 * Allows to create a ChronoGraph instance using a custom user-provided store implementation.
	 *
	 * <p>
	 * This method serves as an extension point for advanced usage. Only required if you want to use a custom backend.
	 * </p>
	 *
	 * @param builderClass   The builder class for the backing store.
	 * @param configureStore Configures the backing key-value-store. Please note that some settings which are mandatory for ChronoGraph will be silently overwritten.
	 *                       Important: do <b>not</b> call {@link ChronoDBFinalizableBuilder#build()} on the builder, ChronoGraph will do this internally.
	 * @param <T>            The type of the builder.
	 * @return The graph builder for further customization on graph-level.
	 */
	public <T extends ChronoDBBackendBuilder> ChronoGraphFinalizableBuilder customGraph(Class<T> builderClass, Function<T, ChronoDBFinalizableBuilder<?>> configureStore);

}
