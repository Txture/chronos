package org.chronos.chronograph.internal.impl.optimizer.step;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;
import org.chronos.chronograph.internal.impl.util.ChronoGraphStepUtil;
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ChronoGraphStep<S, E extends Element> extends GraphStep<S, E> {

    private final List<FilterStep<E>> indexableSubsteps = Lists.newArrayList();

    // =====================================================================================================================
    // CONSTRUCTOR
    // =====================================================================================================================

    public ChronoGraphStep(final GraphStep<S, E> originalStep, final List<FilterStep<E>> indexableSteps) {
        super(
            originalStep.getTraversal(),
            originalStep.getReturnClass(),
            // IMPORTANT: the ChronoGraphStep is ALWAYS a "start" step, i.e. it is iterable only ONCE!
            // if this should ever change, please update the "isStartStep" condition in the ChronoGraphStepStrategy!
            true,
            originalStep.getIds()
        );
        // copy the labels of the original step
        originalStep.getLabels().forEach(this::addLabel);
        // add the sub-steps...
        this.indexableSubsteps.addAll(indexableSteps);
        // ... and copy their labels
        indexableSteps.forEach(subStep -> subStep.getLabels().forEach(this::addLabel));
        // set the result iterator supplier function (i.e. the function that calculates the result of this step).
        // We have to do this computation eagerly here because the step strategy holds the read lock on the index
        // while this object is created.
        List<E> results = this.getResultList();
        this.setIteratorSupplier(results::iterator);
    }

    // =====================================================================================================================
    // PUBLIC API
    // =====================================================================================================================

    @Override
    public String toString() {
        // according to TinkerGraph reference implementation
        if (this.indexableSubsteps.isEmpty()) {
            return super.toString();
        } else {
            return 0 == this.ids.length
                ? StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.indexableSubsteps)
                : StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(),
                Arrays.toString(this.ids), this.indexableSubsteps);
        }
    }

    // =====================================================================================================================
    // ITERATION & STEP RESULT CALCULATION
    // =====================================================================================================================

    @SuppressWarnings("unchecked")
    private List<E> getResultList() {
        if (Vertex.class.isAssignableFrom(this.returnClass)) {
            return (List<E>) this.getResultVertices();
        } else {
            return (List<E>) this.getResultEdges();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<Vertex> getResultVertices() {
        ChronoGraph graph = ChronoGraphTraversalUtil.getChronoGraph(this.getTraversal());
        // ensure that we have an open transaction...
        graph.tx().readWrite();
        // ... and retrieve it
        ChronoGraphTransaction tx = ChronoGraphTraversalUtil.getTransaction(this.getTraversal());

        Set<ChronoGraphIndex> cleanIndices = graph.getIndexManagerOnBranch(tx.getBranchName()).getCleanIndicesAtTimestamp(tx.getTimestamp());
        // start the query builder
        ChronoDBTransaction dbTx = tx.getBackingDBTransaction();
        QueryBuilder queryBuilder = dbTx.find().inKeyspace(ChronoGraphConstants.KEYSPACE_VERTEX);

        Set<Vertex> verticesFromIndexQuery = getVerticesFromChronoDB(tx, cleanIndices, queryBuilder);

        // consider the transaction context
        GraphTransactionContextInternal context = (GraphTransactionContextInternal) tx.getContext();
        if (!context.isDirty()) {
            // return the index query result directly
            return Lists.newArrayList(verticesFromIndexQuery);
        }

        // the context is dirty; we have to run the predicate over all modified vertices as well
        Predicate<Vertex> predicate = (Predicate<Vertex>) ChronoGraphTraversalUtil.filterStepsToPredicate(this.indexableSubsteps);
        Set<String> queryProperties = ChronoGraphTraversalUtil.getHasPropertyKeys(this.indexableSubsteps);

        Set<Vertex> combinedVertices = Sets.newHashSet(verticesFromIndexQuery);
        combinedVertices.addAll(context.getVerticesWithModificationsOnProperties(queryProperties));

        return combinedVertices.stream()
            // eliminate all vertices which have been removed in this transaction
            .filter(v -> ((ChronoVertex) v).isRemoved() == false)
            .filter(predicate)
            .collect(Collectors.toList());
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private Set<Vertex> getVerticesFromChronoDB(final ChronoGraphTransaction tx, final Set<ChronoGraphIndex> cleanIndices, final QueryBuilder queryBuilder) {
        List<FilterStep<E>> chronoDbFilters = this.indexableSubsteps;
        chronoDbFilters = ChronoGraphStepUtil.optimizeFilters(chronoDbFilters);
        // for efficiency, it's better NOT to pass any negated filters down to ChronoDB. The reason is
        // that ChronoDB needs to perform an intersection with the primary index, as all indices are
        // sparse. The better option is to let ChronoDB evaluate the non-negated filters, and we apply
        // the rest of the filters in-memory.

        List<FilterStep<E>> negatedFilters = chronoDbFilters.stream().filter(ChronoGraphStepUtil::isNegated).collect(Collectors.toList());
        List<FilterStep<E>> nonNegatedFilters = chronoDbFilters.stream().filter(step -> !ChronoGraphStepUtil.isNegated(step)).collect(Collectors.toList());

        Set<Vertex> verticesFromIndexQuery;
        if (nonNegatedFilters.isEmpty()) {
            // all filters are negated, this query will be slow
            // there's no reason to run ALL negated queries against the index, it will only
            // result in needless checks against the primary index, which will slow things down. Only
            // run ONE of the negated queries on the index, and the rest in-memory.
            FilterStep<E> firstFilter = Iterables.getFirst(negatedFilters, null);
            List<FilterStep<E>> indexQueries = Lists.newArrayList();
            indexQueries.add(firstFilter);

            FinalizableQueryBuilder finalizableQueryBuilder = ChronoGraphTraversalUtil.toChronoDBQuery(
                cleanIndices,
                indexQueries,
                queryBuilder,
                ChronoGraphTraversalUtil::createIndexKeyForVertexProperty
            );

            Iterator<QualifiedKey> keys = finalizableQueryBuilder.getKeys();
            verticesFromIndexQuery = Streams.stream(keys)
                .map(QualifiedKey::getKey)
                .map(id -> tx.getVertexOrNull(id, ElementLoadMode.LAZY))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

            // there is a slight difference in query semantics between ChronoDB and Gremlin when it comes to NEGATED predicates:
            // - In ChronoDB, a key is returned if its value matches the negated predicate. Note that "null" matches many negated predicates.
            // - In Gremlin, a graph element is returned if it HAS a value AND that value matches the negated predicate.
            // We therefore need to apply a post-processing here, checking that the vertices indeed have the requested property keys.
            Predicate<Vertex> predicate = (Predicate<Vertex>) ChronoGraphTraversalUtil.filterStepsToPredicate(negatedFilters);
            verticesFromIndexQuery = verticesFromIndexQuery.stream().filter(predicate).collect(Collectors.toSet());

        } else {
            // run the non-negated filters on ChronoDB, apply the rest in-memory
            FinalizableQueryBuilder finalizableQueryBuilder = ChronoGraphTraversalUtil.toChronoDBQuery(
                cleanIndices,
                nonNegatedFilters,
                queryBuilder,
                ChronoGraphTraversalUtil::createIndexKeyForVertexProperty
            );
            Iterator<QualifiedKey> keys = finalizableQueryBuilder.getKeys();
            verticesFromIndexQuery = Streams.stream(keys)
                .map(QualifiedKey::getKey)
                .map(id -> tx.getVertexOrNull(id, ElementLoadMode.LAZY))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

            // check if we have negated filters
            if (!negatedFilters.isEmpty()) {
                Predicate<Vertex> predicate = (Predicate<Vertex>) ChronoGraphTraversalUtil.filterStepsToPredicate(negatedFilters);
                verticesFromIndexQuery = verticesFromIndexQuery.stream().filter(predicate).collect(Collectors.toSet());
            }
        }
        return verticesFromIndexQuery;
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<Edge> getResultEdges() {
        ChronoGraph graph = ChronoGraphTraversalUtil.getChronoGraph(this.getTraversal());
        // ensure that we have an open transaction...
        graph.tx().readWrite();
        // ... and retrieve it
        ChronoGraphTransaction tx = ChronoGraphTraversalUtil.getTransaction(this.getTraversal());

        Set<ChronoGraphIndex> cleanIndices = graph.getIndexManagerOnBranch(tx.getBranchName()).getCleanIndicesAtTimestamp(tx.getTimestamp());
        // start the query builder
        ChronoDBTransaction dbTx = tx.getBackingDBTransaction();
        QueryBuilder queryBuilder = dbTx.find().inKeyspace(ChronoGraphConstants.KEYSPACE_EDGE);

        Set<Edge> edgesFromIndexQuery = getEdgesFromChronoDB(tx, cleanIndices, queryBuilder);

        // consider the transaction context
        GraphTransactionContextInternal context = (GraphTransactionContextInternal) tx.getContext();
        if (!context.isDirty()) {
            // return the index query result directly
            return Lists.newArrayList(edgesFromIndexQuery);
        }

        // the context is dirty; we have to run the predicate over all modified vertices as well
        Predicate<Edge> predicate = (Predicate<Edge>) ChronoGraphTraversalUtil.filterStepsToPredicate(this.indexableSubsteps);
        Set<String> queryProperties = ChronoGraphTraversalUtil.getHasPropertyKeys(this.indexableSubsteps);

        Set<Edge> combinedEdges = Sets.newHashSet(edgesFromIndexQuery);
        combinedEdges.addAll(context.getEdgesWithModificationsOnProperties(queryProperties));

        return combinedEdges.stream()
            // eliminate all vertices which have been removed in this transaction
            .filter(v -> ((ChronoEdge) v).isRemoved() == false)
            .filter(predicate)
            .collect(Collectors.toList());
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private Set<Edge> getEdgesFromChronoDB(final ChronoGraphTransaction tx, final Set<ChronoGraphIndex> cleanIndices, final QueryBuilder queryBuilder) {
        List<FilterStep<E>> chronoDbFilters = this.indexableSubsteps;
        chronoDbFilters = ChronoGraphStepUtil.optimizeFilters(chronoDbFilters);
        // for efficiency, it's better NOT to pass any negated filters down to ChronoDB. The reason is
        // that ChronoDB needs to perform an intersection with the primary index, as all indices are
        // sparse. The better option is to let ChronoDB evaluate the non-negated filters, and we apply
        // the rest of the filters in-memory.

        List<FilterStep<E>> negatedFilters = chronoDbFilters.stream().filter(ChronoGraphStepUtil::isNegated).collect(Collectors.toList());
        List<FilterStep<E>> nonNegatedFilters = chronoDbFilters.stream().filter(step -> !ChronoGraphStepUtil.isNegated(step)).collect(Collectors.toList());

        Set<Edge> edgesFromIndexQuery;
        if (nonNegatedFilters.isEmpty()) {
            // all filters are negated, this query will be slow
            // there's no reason to run ALL negated queries against the index, it will only
            // result in needless checks against the primary index, which will slow things down. Only
            // run ONE of the negated queries on the index, and the rest in-memory.
            FilterStep<E> firstFilter = Iterables.getFirst(negatedFilters, null);
            List<FilterStep<E>> indexQueries = Lists.newArrayList();
            indexQueries.add(firstFilter);

            FinalizableQueryBuilder finalizableQueryBuilder = ChronoGraphTraversalUtil.toChronoDBQuery(
                cleanIndices,
                indexQueries,
                queryBuilder,
                ChronoGraphTraversalUtil::createIndexKeyForEdgeProperty
            );

            Iterator<QualifiedKey> keys = finalizableQueryBuilder.getKeys();
            edgesFromIndexQuery = Streams.stream(keys)
                .map(QualifiedKey::getKey)
                .map(id -> tx.getEdgeOrNull(id, ElementLoadMode.LAZY))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

            // there is a slight difference in query semantics between ChronoDB and Gremlin when it comes to NEGATED predicates:
            // - In ChronoDB, a key is returned if its value matches the negated predicate. Note that "null" matches many negated predicates.
            // - In Gremlin, a graph element is returned if it HAS a value AND that value matches the negated predicate.
            // We therefore need to apply a post-processing here, checking that the vertices indeed have the requested property keys.
            Predicate<Edge> predicate = (Predicate<Edge>) ChronoGraphTraversalUtil.filterStepsToPredicate(negatedFilters);
            edgesFromIndexQuery = edgesFromIndexQuery.stream().filter(predicate).collect(Collectors.toSet());

        } else {
            // run the non-negated filters on ChronoDB, apply the rest in-memory
            FinalizableQueryBuilder finalizableQueryBuilder = ChronoGraphTraversalUtil.toChronoDBQuery(
                cleanIndices,
                nonNegatedFilters,
                queryBuilder,
                ChronoGraphTraversalUtil::createIndexKeyForEdgeProperty
            );
            Iterator<QualifiedKey> keys = finalizableQueryBuilder.getKeys();
            edgesFromIndexQuery = Streams.stream(keys)
                .map(QualifiedKey::getKey)
                .map(id -> tx.getEdgeOrNull(id, ElementLoadMode.LAZY))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

            // check if we have negated filters
            if (!negatedFilters.isEmpty()) {
                Predicate<Edge> predicate = (Predicate<Edge>) ChronoGraphTraversalUtil.filterStepsToPredicate(negatedFilters);
                edgesFromIndexQuery = edgesFromIndexQuery.stream().filter(predicate).collect(Collectors.toSet());
            }
        }
        return edgesFromIndexQuery;
    }

}
