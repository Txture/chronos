package org.chronos.chronodb.internal.impl.migration;

import com.google.common.collect.Lists;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.migration.ChronosMigration;
import org.chronos.chronodb.internal.api.migration.MigrationChain;
import org.chronos.chronodb.internal.api.migration.annotations.Migration;
import org.chronos.common.exceptions.ChronosIOException;
import org.chronos.common.version.ChronosVersion;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public final class MigrationChainImpl<DBTYPE extends ChronoDBInternal> implements MigrationChain<DBTYPE> {

    private static final Logger log = LoggerFactory.getLogger(MigrationChainImpl.class);

    // =================================================================================================================
    // STATIC FACTORY METHODS
    // =================================================================================================================

    private static final Map<String, MigrationChain<?>> PACKAGE_TO_MIGRATION_CHAIN = new HashMap<>();

    @SuppressWarnings("unchecked")
    public static synchronized <DBTYPE extends ChronoDBInternal> MigrationChain<DBTYPE> fromPackage(final String qualifiedPackageName) {
        checkNotNull(qualifiedPackageName, "Precondition violation - argument 'qualifiedPackageName' must not be NULL!");
        MigrationChain<?> cachedChain = PACKAGE_TO_MIGRATION_CHAIN.get(qualifiedPackageName);
        if (cachedChain != null) {
            return (MigrationChain<DBTYPE>) cachedChain;
        }
        try {
            Set<ClassInfo> topLevelClasses = ClassPath.from(Thread.currentThread().getContextClassLoader())
                // get the top level classes of the migration package
                .getTopLevelClasses(qualifiedPackageName);
            Set<Class<? extends ChronosMigration<DBTYPE>>> classes = topLevelClasses
                // stream the class info objects
                .stream()
                // load the classes
                .map(ClassInfo::load)
                // only consider @Migration classes
                .filter(clazz -> clazz.getAnnotation(Migration.class) != null)
                // only consider subclasses of ChronosMigration
                .filter(ChronosMigration.class::isAssignableFrom)
                // cast the classes to subclass of chronos migration
                .map(clazz -> (Class<? extends ChronosMigration<DBTYPE>>) clazz)
                // collect them in a set
                .collect(Collectors.toSet());
            MigrationChain<DBTYPE> chain = new MigrationChainImpl<>(classes);
            PACKAGE_TO_MIGRATION_CHAIN.put(qualifiedPackageName, chain);
            return chain;
        } catch (IOException e) {
            throw new ChronosIOException("Failed to scan classpath for ChronosMigration classes! See root cause for details.", e);
        }
    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final List<Class<? extends ChronosMigration<DBTYPE>>> classes;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    private MigrationChainImpl(final Collection<Class<? extends ChronosMigration<DBTYPE>>> migrationClasses) {
        checkNotNull(migrationClasses, "Precondition violation - argument 'migrationClasses' must not be NULL!");
        List<Class<? extends ChronosMigration<DBTYPE>>> classes = Lists.newArrayList(migrationClasses);
        classes.sort(MigrationClassComparator.INSTANCE);
        this.classes = Collections.unmodifiableList(classes);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @NotNull
    @Override
    public Iterator<Class<? extends ChronosMigration<DBTYPE>>> iterator() {
        return this.classes.iterator();
    }

    @Override
    public List<Class<? extends ChronosMigration<DBTYPE>>> getMigrationClasses() {
        return this.classes;
    }

    @Override
    public MigrationChain<DBTYPE> startingAt(final ChronosVersion from) {
        return new MigrationChainImpl<>(this.classes.stream().filter(clazz -> {
            ChronosVersion classFrom = ChronosVersion.parse(clazz.getAnnotation(Migration.class).from());
            return classFrom.compareTo(from) >= 0;
        }).collect(Collectors.toList()));
    }

    @Override
    public void execute(final DBTYPE chronoDB) {
        for (Class<? extends ChronosMigration<DBTYPE>> migrationClass : this) {
            // create an instance of the migration
            final ChronosMigration<DBTYPE> migration = instantiateMigration(migrationClass);
            try {
                log.info("Migrating ChronoDB from " + MigrationClassUtil.from(migrationClass) + " to " + MigrationClassUtil.to(migrationClass) + " ...");
                // execute the migration
                migration.execute(chronoDB);
                // update the chronos version
                chronoDB.updateChronosVersionTo(MigrationClassUtil.to(migrationClass));
                log.info("Migration of ChronoDB from " + MigrationClassUtil.from(migrationClass) + " to " + MigrationClassUtil.to(migrationClass) + " completed successfully.");
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to execute migration of class '" + migrationClass.getName() + "'!", t);
            }
        }
    }

    @NotNull
    private ChronosMigration<DBTYPE> instantiateMigration(final Class<? extends ChronosMigration<DBTYPE>> migrationClass) {
        final ChronosMigration<DBTYPE> migration;
        try {
            migration = migrationClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Failed to instantiate migration class '" + migrationClass.getName() + "'!", e);
        }
        return migration;
    }

}
