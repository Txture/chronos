package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.chronos.chronodb.api.builder.database.*;
import org.chronos.chronodb.api.exceptions.ChronoDBConfigurationException;
import org.chronos.common.builder.ChronoBuilder;

public class ChronoDBBaseBuilderImpl extends AbstractChronoDBBuilder<ChronoDBBaseBuilderImpl>
		implements ChronoDBBaseBuilder {

	@Override
	public ChronoDBPropertyFileBuilder fromPropertiesFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		return new ChronoDBPropertyFileBuilderImpl(file);
	}

	@Override
	public ChronoDBPropertyFileBuilder fromPropertiesFile(final String filePath) {
		checkNotNull(filePath, "Precondition violation - argument 'filePath' must not be NULL!");
		return new ChronoDBPropertyFileBuilderImpl(filePath);
	}

	@Override
	public ChronoDBPropertyFileBuilder fromConfiguration(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		return new ChronoDBPropertyFileBuilderImpl(configuration);
	}

	@Override
	public ChronoDBPropertyFileBuilder fromProperties(final Properties properties) {
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		Configuration configuration = new MapConfiguration(properties);
		return this.fromConfiguration(configuration);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends ChronoDBBackendBuilder> T database(final Class<T> builderClass) {
		checkNotNull(builderClass, "Precondition violation - argument 'builderClass' must not be NULL!");
		try {
			Constructor<? extends ChronoDBBackendBuilder> constructor = builderClass.getConstructor();
			return (T) constructor.newInstance();
		} catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
			throw new ChronoDBConfigurationException("Failed to instantiate Database Builder of type '" + builderClass.getName() + "'!", e);
		}
	}

}
