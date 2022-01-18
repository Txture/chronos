package org.chronos.chronodb.test.cases.builder;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.impl.cache.bogus.ChronoDBBogusCache;
import org.chronos.chronodb.internal.impl.cache.headfirst.HeadFirstCache;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.exceptions.ChronosConfigurationException;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ChronoDBConfigurationTest extends ChronoDBUnitTest {

    @Test
    public void canCreateChronoDbWithBuilder() {
        ChronoDB db = ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER).build();
        assertNotNull(db);
    }

    @Test
    public void canEnableCachingWithInMemoryBuilder() {
        ChronoDB db = ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER).withLruCacheOfSize(1000).build();
        assertNotNull(db);
        try {
            this.assertHasCache(db);
        } finally {
            db.close();
        }
    }

    @Test
    public void canEnableCachingViaPropertiesFile() {
        File propertiesFile = this.getSrcTestResourcesFile("chronoCacheConfigTest_correct.properties");
        ChronoDB chronoDB = ChronoDB.FACTORY.create().fromPropertiesFile(propertiesFile).build();
        assertNotNull(chronoDB);
        try {
            this.assertHasCache(chronoDB);
        } finally {
            chronoDB.close();
        }
    }

    @Test
    public void cacheMaxSizeSettingIsRequiredIfCachingIsEnabled() {
        File propertiesFile = this.getSrcTestResourcesFile("chronoCacheConfigTest_wrong.properties");
        try {
            ChronoDB.FACTORY.create().fromPropertiesFile(propertiesFile).build();
            fail("Managed to create cached ChronoDB instance without specifying the Max Cache Size!");
        } catch (ChronosConfigurationException expected) {
            // pass
        }
    }

    @Test
    public void canConfigureHeadFirstCache() {
        ChronoDB db = ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER)
            .withProperty(ChronoDBConfiguration.CACHING_ENABLED,"true")
            .withProperty(ChronoDBConfiguration.CACHE_TYPE,"head-first")
            .withProperty(ChronoDBConfiguration.CACHE_MAX_SIZE, "100")
            .withProperty(ChronoDBConfiguration.CACHE_HEADFIRST_PREFERRED_BRANCH, "master")
            .withProperty(ChronoDBConfiguration.CACHE_HEADFIRST_PREFERRED_KEYSPACE, "vertex")
            .build();
        ChronoDBCache cache = db.getCache();
        assertTrue(cache instanceof HeadFirstCache);
        HeadFirstCache hfc = (HeadFirstCache) cache;
        assertEquals("master", hfc.getPreferredBranch());
        assertEquals("vertex", hfc.getPreferredKeyspace());
    }

    private void assertHasCache(final ChronoDB db) {
        // make sure that we have a cache
        assertNotNull(db.getCache());
        // make sure it's not a bogus cache
        assertFalse(db.getCache() instanceof ChronoDBBogusCache);
    }
}
