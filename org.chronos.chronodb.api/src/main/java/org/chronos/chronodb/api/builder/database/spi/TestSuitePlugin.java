package org.chronos.chronodb.api.builder.database.spi;

import org.apache.commons.configuration2.Configuration;
import org.chronos.common.exceptions.ChronosIOException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public interface TestSuitePlugin {

    public Configuration createBasicTestConfiguration(Method testMethod, File testDirectory);

    public void onBeforeTest(Class<?> testClass, Method testMethod, File testDirectory);

    public void onAfterTest(Class<?> testClass, Method testMethod, File testDirectory);

    public default File createFileDBFile(File testDirectory) {
        File dbFile = new File(testDirectory, UUID.randomUUID().toString().replaceAll("-", "") + ".chronodb");
        try {
            Path path = dbFile.toPath();
            Files.deleteIfExists(path);
            Files.createFile(path);
        } catch (IOException e) {
            throw new ChronosIOException("Failed to create DB file in test directory '" + testDirectory.getAbsolutePath() + "'!");
        }
        return dbFile;
    }
}
