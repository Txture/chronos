package org.chronos.chronodb.api.builder.database;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.MaintenanceManager;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.common.builder.ChronoBuilder;

/**
 * This is the starter interface for building a {@link ChronoDB} interface in a fluent API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBBaseBuilder {

	/**
	 * Loads the given file as a {@link Properties} file, and assigns it to a provider.
	 * <p>
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param file The file to load. Must exist, must be a file, must not be <code>null</code>.
	 * @return The provider to continue configuration with, with the properties from the given file loaded. Never
	 * <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromPropertiesFile(File file);

	/**
	 * Loads the given file path as a {@link Properties} file, and assigns it to a provider.
	 * <p>
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param filePath The file path to load. Must exist, must be a file, must not be <code>null</code>.
	 * @return The provider to continue configuration with, with the properties from the given file loaded. Never
	 * <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromPropertiesFile(String filePath);

	/**
	 * Loads the given Apache {@link Configuration} object and assigns it to a provider.
	 * <p>
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param configuration The configuration data to load. Must not be <code>null</code>.
	 * @return The provider to continue configuration with, with the properties from the configuration loaded. Never
	 * <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromConfiguration(Configuration configuration);

	/**
	 * Loads the given {@link java.util.Properties} object and assigns it to a provider.
	 * <p>
	 * <p>
	 * For details on the possible configurations, please refer to {@linkplain ChronoDBConfiguration the configuration
	 * API}.
	 *
	 * @param properties The properties to load. Must not be <code>null</code>.
	 * @return The provider to continue configuration with, with the given properties loaded. Never <code>null</code>.
	 */
	public ChronoDBPropertyFileBuilder fromProperties(Properties properties);

	/**
	 * Creates a new database instance using the given provider class.
     *
     * <p>
     * For example, to create an in-memory database, use:
     *
     * <pre>
     * ChronoDB inMemoryDB = ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER).build()
     * </pre>
     * </p>
	 *
	 * @param builderClass The provider class to use. Please refer to the documentation of your backend to find the correct class. Must not be <code>null</code>.
	 * @param <T>          The provider type produced by the given class.
	 * @return The provider to continue configuration with, never <code>null</code>.
	 */
	public <T extends ChronoDBBackendBuilder> T database(Class<T> builderClass);

}
