package org.chronos.chronograph.test.cases.system;

import org.apache.commons.io.FileUtils;
import org.chronos.chronodb.exodus.builder.ExodusChronoDBBuilderImpl;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class ChronoGraphFactoryTest {

    @Test
    public void canCreateInMemoryDatabaseEasily() {
        // without additional configuration
        try (ChronoGraph graph = ChronoGraph.FACTORY.create().inMemoryGraph().build()) {
            assertNotNull(graph);
        }
        // with additional configuration
        try (ChronoGraph graph = ChronoGraph.FACTORY.create().inMemoryGraph(config -> config.assumeCachedValuesAreImmutable(true)).build()) {
            assertNotNull(graph);
        }
    }

    @Test
    public void canCreateExodusDatabaseEasily() throws Exception {
        File directory = Files.createTempDirectory("chronodb-test").toFile();
        try {
            // without additional configuration
            try (ChronoGraph graph = ChronoGraph.FACTORY.create().exodusGraph(directory.getAbsolutePath()).build()) {
                assertNotNull(graph);
            }
            // with additional configuration
            try (ChronoGraph graph = ChronoGraph.FACTORY.create().exodusGraph(directory.getAbsolutePath(), config -> config.assumeCachedValuesAreImmutable(true)).build()) {
                assertNotNull(graph);
            }
        } finally {
            FileUtils.deleteDirectory(directory);
        }
    }

    @Test
    public void canCreateCustomDatabaseEasily() throws Exception {
        File directory = Files.createTempDirectory("chronodb-test").toFile();
        try {
            try (ChronoGraph graph = ChronoGraph.FACTORY.create().customGraph(ExodusChronoDBBuilderImpl.class, (ExodusChronoDBBuilderImpl builder) -> builder.onFile(directory)).build()) {
                assertNotNull(graph);
            }
        } finally {
            FileUtils.deleteDirectory(directory);
        }
    }


}
