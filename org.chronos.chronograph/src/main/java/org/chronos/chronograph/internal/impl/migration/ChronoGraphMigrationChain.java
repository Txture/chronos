package org.chronos.chronograph.internal.impl.migration;

import com.google.common.collect.Lists;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import org.apache.commons.io.FilenameUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat;
import org.chronos.chronodb.internal.api.stream.ObjectInput;
import org.chronos.chronodb.internal.api.stream.ObjectOutput;
import org.chronos.chronodb.internal.impl.dump.ChronoDBDumpUtil;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronograph.internal.api.migration.ChronoGraphMigration;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.version.ChronosVersion;
import org.chronos.common.version.VersionKind;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphMigrationChain {

    private static final List<Class<? extends ChronoGraphMigration>> ALL_CHRONOGRAPH_MIGRATION_CLASSES = Collections.unmodifiableList(Lists.newArrayList(
        ChronoGraphMigration_0_11_1_to_0_11_2.class,
        ChronoGraphMigration_0_11_6_to_0_11_7.class
    ));

    private static final List<ChronoGraphMigration> ALL_GRAPH_MIGRATIONS_IN_ORDER;

    static {
        ALL_GRAPH_MIGRATIONS_IN_ORDER = Collections.unmodifiableList(loadMigrationsInAscendingOrder());
    }


    public static void executeMigrationChainOnGraph(ChronoGraphInternal graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        Optional<ChronosVersion> currentVersion = graph.getStoredChronoGraphVersion();
        if (!currentVersion.isPresent()) {
            throw new IllegalStateException("Failed to execute ChronoGraph migration chain - current version is unknown!");
        }
        List<ChronoGraphMigration> migrationsToApply = ALL_GRAPH_MIGRATIONS_IN_ORDER.stream()
            .filter(m -> m.getFromVersion().isGreaterThanOrEqualTo(currentVersion.get()))
            .collect(Collectors.toList());
        if (migrationsToApply.isEmpty()) {
            return;
        }
        // make sure we're not read-only.
        if (graph.getBackingDB().getConfiguration().isReadOnly()) {
            throw new IllegalStateException("You are trying to open this database in read-only mode with ChronoGraph " + ChronosVersion.getCurrentVersion() + ". " +
                "However, this database has been written with an older format (version " + currentVersion.get() + ") and " + migrationsToApply.size() + " data format " +
                "migrations needs to be applied in order for this ChronoGraph version to work properly. Please either open the graph in read-write mode to apply the " +
                "required data migraitons (breaking compatibility with older ChronoGraph versions), or use an older ChronoGraph binary to access this database.");
        }
        for (ChronoGraphMigration migration : migrationsToApply) {
            migration.execute(graph);
            // the migration was executed successfully, write the new
            // version to the graph
            graph.setStoredChronoGraphVersion(migration.getToVersion());
        }
    }

    public static File executeMigrationChainOnDumpFile(File dumpFile, DumpOptions options) {
        checkNotNull(dumpFile, "Precondition violation - argument 'dumpFile' must not be NULL!");
        checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!");
        try (ObjectInput input = ChronoDBDumpFormat.createInput(dumpFile, options)) {
            String extension = FilenameUtils.getExtension(dumpFile.getName());
            File outputFile = Files.createTempFile(dumpFile.getName() + "_graphMigrated", "." + extension).toFile();
            try (ObjectOutput output = ChronoDBDumpFormat.createOutput(outputFile, options)) {
                // the first entry in a dump is always the metadata
                ChronoDBDumpMetadata metadata = ChronoDBDumpUtil.readMetadata(input);
                ChronosVersion dumpVersion = metadata.getChronosVersion();
                // load all existing ChronoGraph migrations and keep the ones which are required for this version
                List<ChronoGraphMigration> migrationsToApply = ALL_GRAPH_MIGRATIONS_IN_ORDER.stream()
                    .filter(m -> m.getFromVersion().isGreaterThanOrEqualTo(dumpVersion))
                    .collect(Collectors.toList());
                // migrate the metadata object
                for (ChronoGraphMigration migration : migrationsToApply) {
                    migration.execute(metadata);
                }
                // write the metadata into the output
                output.write(metadata);
                // for the remaining entries, migrate them one by one and write them to the output.
                while (input.hasNext()) {
                    Object entry = input.next();
                    if (entry instanceof ChronoDBDumpEntry == false) {
                        // no idea what this is
                        throw new IllegalStateException("Encountered unknown entry in DB dump of type '" + entry.getClass().getName() + "'.");
                    }
                    ChronoDBDumpEntry<?> dumpEntry = (ChronoDBDumpEntry<?>) entry;
                    for (ChronoGraphMigration migration : migrationsToApply) {
                        dumpEntry = migration.execute(dumpEntry);
                        if (dumpEntry == null) {
                            // migration decided to discard this entry
                            break;
                        }
                    }
                    if (dumpEntry != null) {
                        // entry passed all migrations, add to migrated dump
                        output.write(dumpEntry);
                    }
                }
            }
            return outputFile;
        } catch (IOException ioe) {
            throw new ChronosIOException("Could not migrate DB dump due to an I/O error: " + ioe, ioe);
        }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    @SuppressWarnings({"unchecked", "UnstableApiUsage"})
    private static List<ChronoGraphMigration> loadMigrationsInAscendingOrder() {
        List<ChronoGraphMigration> allMigrations = ALL_CHRONOGRAPH_MIGRATION_CLASSES.stream()
            .distinct()
            .map(c -> instantiateMigration((Class<? extends ChronoGraphMigration>) c))
            .sorted(Comparator.comparing(ChronoGraphMigration::getFromVersion))
            .collect(Collectors.toList());

        // make sure from and to are strictly monotonically increasing
        for (ChronoGraphMigration migration : allMigrations) {
            if (!migration.getFromVersion().isSmallerThan(migration.getToVersion())) {
                throw new IllegalStateException("ChronoGraph Migration '" + migration.getClass().getName() + "' is invalid: " +
                    "it specifies a lower 'to' version (" + migration.getToVersion() + ") than " +
                    "'from' version (" + migration.getFromVersion() + ")!");
            }
        }

        // make sure there are no gaps or overlaps
        if (allMigrations.size() >= 2) {
            for (int i = 0; i < allMigrations.size() - 1; i++) {
                ChronoGraphMigration m = allMigrations.get(i);
                ChronoGraphMigration mPlusOne = allMigrations.get(i + 1);
                if (!m.getToVersion().isSmallerThan(mPlusOne.getFromVersion())) {
                    throw new IllegalStateException("ChronoGraph Migration Chain is inconsistent: " +
                        "migration [" + m.getClass().getName() + "] ends at version " + m.getToVersion() + ", " +
                        "but the next migration [" + m.getClass().getName() + "] starts at version " + m.getFromVersion() + "!");
                }
            }
        }

        // make sure there are only migrations to releases
        for (ChronoGraphMigration migration : allMigrations) {
            if (migration.getFromVersion().getVersionKind() != VersionKind.RELEASE) {
                throw new IllegalStateException("ChronoGraph Migration [" + migration.getClass().getName() + "] " +
                    "is invalid: it specifies a 'from' version (" + migration.getFromVersion() + ") " +
                    "which is not a RELEASE!");
            }
            if (migration.getToVersion().getVersionKind() != VersionKind.RELEASE) {
                throw new IllegalStateException("ChronoGraph Migration [" + migration.getClass().getName() + "] " +
                    "is invalid: it specifies a 'to' version (" + migration.getToVersion() + ") " +
                    "which is not a RELEASE!");
            }
        }

        return allMigrations;
    }

    private static <
        T extends ChronoGraphMigration> T instantiateMigration(Class<T> migrationClazz) {
        try {
            Constructor<T> constructor = migrationClazz.getConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("The ChronoGraphMigration class '" + migrationClazz.getName() + "' has no default constructor! Please add a no-argument constructor.", e);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to instantiate ChronoGrpahMigration class '" + migrationClazz.getName() + "'!");
        }
    }
}
