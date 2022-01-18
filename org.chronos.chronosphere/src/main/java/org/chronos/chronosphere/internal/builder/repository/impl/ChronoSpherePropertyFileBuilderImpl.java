package org.chronos.chronosphere.internal.builder.repository.impl;

import com.google.common.collect.Sets;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.chronos.chronosphere.api.builder.repository.ChronoSpherePropertyFileBuilder;
import org.chronos.chronosphere.api.exceptions.ChronoSphereConfigurationException;

import java.io.File;
import java.io.FileReader;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ChronoSpherePropertyFileBuilderImpl
		extends AbstractChronoSphereFinalizableBuilder<ChronoSpherePropertyFileBuilder>
		implements ChronoSpherePropertyFileBuilder {

	public ChronoSpherePropertyFileBuilderImpl(final File propertiesFile) {
		checkNotNull(propertiesFile, "Precondition violation - argument 'propertiesFile' must not be NULL!");
		checkArgument(propertiesFile.exists(),
				"Precondition violation - argument 'propertiesFile' must refer to an existing file!");
		checkArgument(propertiesFile.isFile(),
				"Precondition violation - argument 'propertiesFile' must refer to a file (not a directory)!");
		try {
			PropertiesConfiguration configuration = new PropertiesConfiguration();
			try(FileReader reader = new FileReader(propertiesFile)){
				configuration.read(reader);
			}
			this.applyConfiguration(configuration);
		} catch (Exception e) {
			throw new ChronoSphereConfigurationException(
					"Failed to read properties file '" + propertiesFile.getAbsolutePath() + "'!", e);
		}
	}

	public ChronoSpherePropertyFileBuilderImpl(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		try {
			this.applyConfiguration(configuration);
		} catch (Exception e) {
			throw new ChronoSphereConfigurationException("Failed to apply the given configuration'!", e);
		}
	}

	private void applyConfiguration(final Configuration configuration) {
		Set<String> keys = Sets.newHashSet(configuration.getKeys());
		for (String key : keys) {
			this.withProperty(key, configuration.getProperty(key).toString());
		}
	}

}
