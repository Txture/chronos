package org.chronos.chronograph.internal.impl.iterators.builder;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.iterators.states.AllChangedEdgeIdsAndTheirPreviousNeighborhoodState;
import org.chronos.chronograph.api.iterators.states.AllChangedEdgeIdsState;
import org.chronos.chronograph.api.iterators.states.AllChangedElementIdsAndTheirNeighborhoodState;
import org.chronos.chronograph.api.iterators.states.AllChangedElementIdsState;
import org.chronos.chronograph.api.iterators.states.AllChangedVertexIdsAndTheirPreviousNeighborhoodState;
import org.chronos.chronograph.api.iterators.states.AllChangedVertexIdsState;
import org.chronos.chronograph.api.iterators.states.AllEdgesState;
import org.chronos.chronograph.api.iterators.states.AllElementsState;
import org.chronos.chronograph.api.iterators.states.AllVerticesState;
import org.chronos.chronograph.api.iterators.states.GraphIteratorState;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllChangedEdgeIdsAndTheirPreviousNeighborhoodStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllChangedEdgeIdsStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllChangedElementIdsAndTheirPreviousNeighborhoodStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllChangedElementIdsStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllChangedVertexIdsAndTheirPreviousNeighborhoodStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllChangedVertexIdsStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllEdgesStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllElementsStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.AllVerticesStateImpl;
import org.chronos.chronograph.internal.impl.iterators.builder.states.GraphIteratorStateImpl;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class ChronoGraphTimestampIteratorBuilderImpl extends AbstractChronoGraphIteratorBuilder implements org.chronos.chronograph.api.iterators.ChronoGraphTimestampIteratorBuilder {

    protected ChronoGraphTimestampIteratorBuilderImpl(BuilderConfig config) {
        super(config);
    }

    @Override
    public void visitCoordinates(final Consumer<GraphIteratorState> consumer) {
        this.iterate(this::createVisitCoordinatesStates, consumer);
    }

    private Iterator<GraphIteratorState> createVisitCoordinatesStates(final ChronoGraph graph) {
        return Iterators.singletonIterator(new GraphIteratorStateImpl(graph));
    }

    @Override
    public void overAllVertices(final Consumer<AllVerticesState> consumer) {
        this.iterate(this::createAllVerticesStates, consumer);
    }

    private Iterator<AllVerticesState> createAllVerticesStates(final ChronoGraph graph) {
        Iterator<Vertex> vertexIterator = graph.vertices();
        return Iterators.transform(vertexIterator, v -> new AllVerticesStateImpl(graph, v));
    }

    @Override
    public void overAllEdges(final Consumer<AllEdgesState> consumer) {
        this.iterate(this::createAllEdgesStates, consumer);
    }

    private Iterator<AllEdgesState> createAllEdgesStates(final ChronoGraph graph) {
        Iterator<Edge> edgeIterator = graph.edges();
        return Iterators.transform(edgeIterator, e -> new AllEdgesStateImpl(graph, e));
    }

    @Override
    public void overAllElements(final Consumer<AllElementsState> consumer) {
        this.iterate(this::createAllElementsStates, consumer);
    }

    private Iterator<AllElementsState> createAllElementsStates(final ChronoGraph graph) {
        Iterator<Vertex> vertexIterator = this.getGraph().vertices();
        Iterator<Edge> edgeIterator = this.getGraph().edges();
        Iterator<Element> elementIterator = Iterators.concat(vertexIterator, edgeIterator);
        return Iterators.transform(elementIterator, e -> new AllElementsStateImpl(graph, e));
    }

    @Override
    public void overAllChangedVertexIds(final Consumer<AllChangedVertexIdsState> consumer) {
        this.iterate(this::createAllChangedVertexIdsStates, consumer);
    }

    private Iterator<AllChangedVertexIdsState> createAllChangedVertexIdsStates(final ChronoGraph graph) {
        String branch = graph.tx().getCurrentTransaction().getBranchName();
        long timestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<String> changedVertexIds = this.getGraph().getChangedVerticesAtCommit(branch, timestamp);
        return Iterators.transform(changedVertexIds, vId -> new AllChangedVertexIdsStateImpl(graph, vId));
    }

    @Override
    public void overAllChangedEdgeIds(final Consumer<AllChangedEdgeIdsState> consumer) {
        this.iterate(this::createAllChangedEdgeIdsStates, consumer);
    }

    private Iterator<AllChangedEdgeIdsState> createAllChangedEdgeIdsStates(final ChronoGraph graph) {
        String branch = graph.tx().getCurrentTransaction().getBranchName();
        long timestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<String> changedEdgeIds = this.getGraph().getChangedEdgesAtCommit(branch, timestamp);
        return Iterators.transform(changedEdgeIds, eId -> new AllChangedEdgeIdsStateImpl(graph, eId));
    }

    @Override
    public void overAllChangedElementIds(final Consumer<AllChangedElementIdsState> consumer) {
        this.iterate(this::createAllChangedElementIdsStates, consumer);
    }

    private Iterator<AllChangedElementIdsState> createAllChangedElementIdsStates(final ChronoGraph graph) {
        String branch = graph.tx().getCurrentTransaction().getBranchName();
        long timestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<String> changedVertexIds = this.getGraph().getChangedVerticesAtCommit(branch, timestamp);
        Iterator<String> changedEdgeIds = this.getGraph().getChangedEdgesAtCommit(branch, timestamp);
        Iterator<AllChangedElementIdsState> vertexStateIterator = Iterators.transform(changedVertexIds, vId -> new AllChangedElementIdsStateImpl(graph, vId, Vertex.class));
        Iterator<AllChangedElementIdsState> edgeStateIterator = Iterators.transform(changedEdgeIds, eId -> new AllChangedElementIdsStateImpl(graph, eId, Edge.class));
        return Iterators.concat(vertexStateIterator, edgeStateIterator);
    }

    @Override
    public void overAllChangedVertexIdsAndTheirPreviousNeighborhood(final Consumer<AllChangedVertexIdsAndTheirPreviousNeighborhoodState> consumer) {
        this.iterate(this::createAllChangedVertexIdsAndTheirPreviousNeighborhoodStates, consumer);
    }

    private Iterator<AllChangedVertexIdsAndTheirPreviousNeighborhoodState> createAllChangedVertexIdsAndTheirPreviousNeighborhoodStates(final ChronoGraph graph) {
        String branch = graph.tx().getCurrentTransaction().getBranchName();
        long timestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<String> vertexIdIterator = graph.getChangedVerticesAtCommit(branch, timestamp);
        return Iterators.transform(vertexIdIterator, vId -> new AllChangedVertexIdsAndTheirPreviousNeighborhoodStateImpl(graph, vId));
    }


    @Override
    public void overAllChangedEdgeIdsAndTheirPreviousNeighborhood(final Consumer<AllChangedEdgeIdsAndTheirPreviousNeighborhoodState> consumer) {
        this.iterate(this::createAllChangedEdgeIdsAndTheirPreviousNeighborhoodStates, consumer);
    }

    private Iterator<AllChangedEdgeIdsAndTheirPreviousNeighborhoodState> createAllChangedEdgeIdsAndTheirPreviousNeighborhoodStates(final ChronoGraph graph) {
        String branch = graph.tx().getCurrentTransaction().getBranchName();
        long timestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<String> edgeIdIterator = graph.getChangedEdgesAtCommit(branch, timestamp);
        return Iterators.transform(edgeIdIterator, vId -> new AllChangedEdgeIdsAndTheirPreviousNeighborhoodStateImpl(graph, vId));
    }

    @Override
    public void overAllChangedElementIdsAndTheirPreviousNeighborhood(final Consumer<AllChangedElementIdsAndTheirNeighborhoodState> consumer) {
        this.iterate(this::createAllChangedElementIdsAndTheirPreviousNeighborhoodStates, consumer);
    }

    private Iterator<AllChangedElementIdsAndTheirNeighborhoodState> createAllChangedElementIdsAndTheirPreviousNeighborhoodStates(final ChronoGraph graph) {
        String branch = graph.tx().getCurrentTransaction().getBranchName();
        long timestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<String> changedVertexIds = this.getGraph().getChangedVerticesAtCommit(branch, timestamp);
        Iterator<String> changedEdgeIds = this.getGraph().getChangedEdgesAtCommit(branch, timestamp);
        Iterator<AllChangedElementIdsAndTheirNeighborhoodState> vertexStatesIterator = Iterators.transform(changedVertexIds, vId -> new AllChangedElementIdsAndTheirPreviousNeighborhoodStateImpl(graph, vId, Vertex.class));
        Iterator<AllChangedElementIdsAndTheirNeighborhoodState> edgeStatesIterator = Iterators.transform(changedEdgeIds, eId -> new AllChangedElementIdsAndTheirPreviousNeighborhoodStateImpl(graph, eId, Edge.class));
        return Iterators.concat(vertexStatesIterator, edgeStatesIterator);
    }


    private <T> void iterate(Function<ChronoGraph, Iterator<T>> stateProducer, Consumer<T> stateConsumer) {
        BuilderConfig c = this.getConfig();
        Iterator<String> branchIterator = c.getBranchNames().iterator();
        // apply the branch change side effect
        branchIterator = SideEffectIterator.create(branchIterator, c.getBranchChangeCallback()::handleBranchChanged);
        while (branchIterator.hasNext()) {
            String branch = branchIterator.next();
            Iterator<Long> timestampsIterator = c.getBranchToTimestampsFunction().apply(branch);
            // apply the timestamp change side effect
            timestampsIterator = SideEffectIterator.create(timestampsIterator, c.getTimestampChangeCallback()::handleTimestampChange);
            while (timestampsIterator.hasNext()) {
                Long timestamp = timestampsIterator.next();
                ChronoGraph graph = c.getGraph();
                ChronoGraph txGraph = graph.tx().createThreadedTx(branch, timestamp);
                try {
                    Iterator<T> stateIterator = stateProducer.apply(txGraph);
                    while (stateIterator.hasNext()) {
                        T state = stateIterator.next();
                        stateConsumer.accept(state);
                    }
                } finally {
                    txGraph.close();
                }
            }
        }
    }
}
