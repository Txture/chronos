package org.chronos.chronograph.internal.impl.builder.graph;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.builder.database.ChronoDBBackendBuilder;
import org.chronos.chronodb.api.builder.database.ChronoDBFinalizableBuilder;
import org.chronos.chronodb.api.builder.database.spi.ChronoDBBackendProvider;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.inmemory.builder.ChronoDBInMemoryBuilder;
import org.chronos.chronodb.internal.impl.base.builder.database.service.ChronoDBBackendProviderService;
import org.chronos.chronograph.api.builder.graph.ChronoGraphBaseBuilder;
import org.chronos.chronograph.api.builder.graph.ChronoGraphFinalizableBuilder;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphBaseBuilderImpl extends AbstractChronoGraphBuilder<ChronoGraphBaseBuilderImpl> implements ChronoGraphBaseBuilder {

    @Override
    public ChronoGraphFinalizableBuilder graphOnChronoDB(final ChronoDBFinalizableBuilder<?> builder) {
        checkNotNull(builder, "Precondition violation - argument 'builder' must not be NULL!");
        return new ChronoGraphOnChronoDBBuilder(builder);
    }

    @Override
    public ChronoGraphFinalizableBuilder fromPropertiesFile(final File file) {
        checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
        checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
        checkArgument(file.isFile(), "Precondition violation - argument 'file' must refer to a file (not a directory)!");
        return new ChronoGraphPropertyFileBuilderImpl(file);
    }

    @Override
    public ChronoGraphFinalizableBuilder fromConfiguration(final Configuration configuration) {
        checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
        return new ChronoGraphPropertyFileBuilderImpl(configuration);
    }

    @Override
    public ChronoGraphFinalizableBuilder fromProperties(final Properties properties) {
        checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
        Configuration configuration = new MapConfiguration(properties);
        return this.fromConfiguration(configuration);
    }

    @Override
    public ChronoGraphFinalizableBuilder inMemoryGraph() {
        return this.graphOnChronoDB(ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER));
    }

    @Override
    public ChronoGraphFinalizableBuilder inMemoryGraph(final Function<ChronoDBInMemoryBuilder, ChronoDBFinalizableBuilder<ChronoDBInMemoryBuilder>> configureStore) {
        checkNotNull(configureStore, "Precondition violation - argument 'configureStore' must not be NULL!");
        ChronoDBInMemoryBuilder dbBuilder = ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER);
        ChronoDBFinalizableBuilder<ChronoDBInMemoryBuilder> configuredBuilder = configureStore.apply(dbBuilder);
        return this.graphOnChronoDB(configuredBuilder);
    }

    @Override
    public ChronoGraphFinalizableBuilder exodusGraph(final String directoryPath) {
        checkNotNull(directoryPath, "Precondition violation - argument 'directoryPath' must not be NULL!");
        return this.exodusGraph(new File(directoryPath));
    }

    @Override
    public ChronoGraphFinalizableBuilder exodusGraph(final File directory) {
        checkArgument(directory.exists(), "Precondition violation - the given directory does not exist: " + directory.getAbsolutePath());
        checkArgument(directory.isDirectory(), "Precondition violation - the given location is not a directory: " + directory.getAbsolutePath());
        return this.exodusGraph(directory, Function.identity());
    }

    @Override
    public ChronoGraphFinalizableBuilder exodusGraph(final String directoryPath, final Function<ChronoDBFinalizableBuilder<?>, ChronoDBFinalizableBuilder<?>> configureStore) {
        checkNotNull(directoryPath, "Precondition violation - argument 'directoryPath' must not be NULL!");
        return this.exodusGraph(new File(directoryPath), configureStore);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public ChronoGraphFinalizableBuilder exodusGraph(final File directory, final Function<ChronoDBFinalizableBuilder<?>, ChronoDBFinalizableBuilder<?>> configureStore) {
        checkArgument(directory.exists(), "Precondition violation - the given directory does not exist: " + directory.getAbsolutePath());
        checkArgument(directory.isDirectory(), "Precondition violation - the given location is not a directory: " + directory.getAbsolutePath());
        checkNotNull(configureStore, "Precondition violation - argument 'configureStore' must not be NULL!");
        // try to access the ChronoDB builder for Exodus. If that fails, it means that it's not on the classpath.
        ChronoDBBackendProvider exodusProvider = ChronoDBBackendProviderService.getInstance().getBackendProvider("exodus");
        if (exodusProvider == null) {
            throw new IllegalStateException("Failed to locate Exodus ChronoDB on classpath. This usually indicates that the" +
                " ChronoDB Exodus dependency is either missing on your classpath, or is outdated. Please check your classpath setup.");
        }
        try {
            ChronoDBBackendBuilder builderInstance = exodusProvider.createBuilder();
            Method onFileMethod = builderInstance.getClass().getDeclaredMethod("onFile", File.class);
            onFileMethod.setAccessible(true);
            ChronoDBFinalizableBuilder<?> finalizableBuilder = (ChronoDBFinalizableBuilder) onFileMethod.invoke(builderInstance, directory);
            ChronoDBFinalizableBuilder<?> configuredBuilder = configureStore.apply(finalizableBuilder);
            return this.graphOnChronoDB(configuredBuilder);
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | ClassCastException e) {
            throw new IllegalStateException("Failed to create Exodus ChronoDB Builder. This usually indicates that the" +
                " ChronoDB Exodus dependency is either missing on your classpath, or is outdated. Please check your classpath setup. The cause is: " + e, e);
        }

    }

    @Override
    public <T extends ChronoDBBackendBuilder> ChronoGraphFinalizableBuilder customGraph(final Class<T> builderClass, final Function<T, ChronoDBFinalizableBuilder<?>> configureStore) {
        checkNotNull(builderClass, "Precondition violation - argument 'builderClass' must not be NULL!");
        checkNotNull(configureStore, "Precondition violation - argument 'configureStore' must not be NULL!");
        try {
            Constructor<T> constructor = builderClass.getConstructor();
            T builder = constructor.newInstance();
            ChronoDBFinalizableBuilder<?> finalizableBuilder = configureStore.apply(builder);
            return this.graphOnChronoDB(finalizableBuilder);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException | ClassCastException e) {
            throw new IllegalStateException("Failed to create custom ChronoDB Builder of type '" + builderClass.getName() + "'. This usually indicates that the" +
                " custom builder does not have a default (no-argument) constructor, or is outdated. Please check your classpath setup. The cause is: " + e, e);
        }
    }

    @Override
    public ChronoGraphFinalizableBuilder fromPropertiesFile(final String filePath) {
        checkNotNull(filePath, "Precondition violation - argument 'filePath' must not be NULL!");
        File file = new File(filePath);
        return this.fromPropertiesFile(file);
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }
}
