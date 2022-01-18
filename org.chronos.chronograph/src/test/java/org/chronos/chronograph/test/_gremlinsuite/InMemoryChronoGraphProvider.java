package org.chronos.chronograph.test._gremlinsuite;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.structure.graph.AbstractChronoElement;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoGraphVariablesImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexProperty;
import org.chronos.chronograph.internal.impl.structure.graph.StandardChronoGraph;
import org.chronos.chronograph.internal.impl.structure.record2.EdgeRecord2;
import org.chronos.chronograph.internal.impl.structure.record2.EdgeTargetRecord2;
import org.chronos.chronograph.internal.impl.structure.record2.PropertyRecord2;
import org.chronos.chronograph.internal.impl.structure.record3.SimpleVertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record3.VertexPropertyRecord3;
import org.chronos.chronograph.internal.impl.structure.record3.VertexRecord3;

import java.util.Map;
import java.util.Set;

public class InMemoryChronoGraphProvider extends AbstractGraphProvider implements GraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final GraphData loadGraphWith) {
        Map<String, Object> baseConfig = Maps.newHashMap();
        baseConfig.put(Graph.GRAPH, ChronoGraph.class.getName());
        baseConfig.put(ChronoDBConfiguration.STORAGE_BACKEND, InMemoryChronoDB.BACKEND_NAME);
        baseConfig.put(ChronoDBConfiguration.MBEANS_ENABLED, false);
        return baseConfig;
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) {
        if (graph == null) {
            return;
        }
        ChronoGraph chronoGraph = (ChronoGraph) graph;
        chronoGraph.close();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Set<Class> getImplementations() {
        Set<Class> implementations = Sets.newHashSet();
        implementations.add(StandardChronoGraph.class);
        implementations.add(ChronoVertexImpl.class);
        implementations.add(ChronoEdgeImpl.class);
        implementations.add(AbstractChronoElement.class);
        implementations.add(ChronoProperty.class);
        implementations.add(ChronoVertexProperty.class);
        implementations.add(ChronoGraphVariablesImpl.class);
        implementations.add(VertexRecord3.class);
        implementations.add(EdgeRecord2.class);
        implementations.add(VertexPropertyRecord3.class);
        implementations.add(SimpleVertexPropertyRecord.class);
        implementations.add(PropertyRecord2.class);
        implementations.add(EdgeTargetRecord2.class);
        return implementations;
    }

    @Override
    public Object convertId(final Object id, final Class<? extends Element> c) {
        return String.valueOf(id);
    }

}
