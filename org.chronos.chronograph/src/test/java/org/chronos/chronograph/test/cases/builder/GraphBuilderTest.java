package org.chronos.chronograph.test.cases.builder;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.test.base.ChronoGraphUnitTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class GraphBuilderTest extends ChronoGraphUnitTest {

    @Test
    public void canCreateGraphOnInMemoryChronoDB(){
        ChronoGraphInternal graph = (ChronoGraphInternal) ChronoGraph.FACTORY.create()
            .graphOnChronoDB(
                ChronoDB.FACTORY.create()
                    .database(InMemoryChronoDB.BUILDER)
                    .withLruCacheOfSize(1000)
            ).withTransactionAutoStart(false).build();
        try{
            assertThat(graph, is(notNullValue()));
            assertThat(graph.getChronoGraphConfiguration().isTransactionAutoOpenEnabled(), is(false));
            assertThat(graph.getBackingDB().getConfiguration().isAssumeCachedValuesAreImmutable(), is(true));
            assertThat(graph.getBackingDB().getConfiguration().isCachingEnabled(), is(true));
            assertThat(graph.getBackingDB().getConfiguration().getCacheMaxSize(), is(1000));
            assertThat(graph.getBackingDB().getConfiguration().getBackendType(), is("inmemory"));
        }finally{
            graph.close();
        }
    }

    @Test
    public void canCreateGraphOnProperties(){
        Properties properties = new Properties();
        properties.put(ChronoDBConfiguration.STORAGE_BACKEND, "inmemory");
        properties.put(ChronoDBConfiguration.CACHING_ENABLED, "true");
        properties.put(ChronoDBConfiguration.CACHE_MAX_SIZE, "1000");
        // note: this is overridden by ChronoGraph default properties.
        properties.put(ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, "false");

        ChronoGraphInternal graph = (ChronoGraphInternal)ChronoGraph.FACTORY.create()
            .fromProperties(properties)
            .withTransactionAutoStart(false).build();
        try{
            assertThat(graph, is(notNullValue()));
            assertThat(graph.getBackingDB().getConfiguration().isAssumeCachedValuesAreImmutable(), is(true));
            assertThat(graph.getBackingDB().getConfiguration().isAssumeCachedValuesAreImmutable(), is(true));
            assertThat(graph.getBackingDB().getConfiguration().isCachingEnabled(), is(true));
            assertThat(graph.getBackingDB().getConfiguration().getCacheMaxSize(), is(1000));
            assertThat(graph.getBackingDB().getConfiguration().getBackendType(), is("inmemory"));
        }finally{
            graph.close();
        }
    }


}
