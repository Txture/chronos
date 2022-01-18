package org.chronos.chronograph.internal.impl.optimizer.step;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronograph.api.builder.query.DoubleWithoutCP;
import org.chronos.chronograph.api.builder.query.LongWithoutCP;
import org.chronos.chronograph.api.builder.query.StringWithoutCP;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;
import org.chronos.chronograph.internal.impl.query.ChronoCompare;
import org.chronos.chronograph.internal.impl.query.ChronoStringCompare;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ChronoGraphStep<S, E extends Element> extends GraphStep<S, E> {

    private static final Set<BiPredicate> NEGATED_PREDICATES = Collections.unmodifiableSet(Sets.newHashSet(
        Compare.neq,
        Contains.without,
        Text.notStartingWith,
        Text.notEndingWith,
        Text.notContaining,
        ChronoCompare.NEQ,
        ChronoCompare.WITHOUT,
        ChronoStringCompare.STRING_NOT_STARTS_WITH,
        ChronoStringCompare.STRING_NOT_STARTS_WITH_IGNORE_CASE,
        ChronoStringCompare.STRING_NOT_ENDS_WITH,
        ChronoStringCompare.STRING_NOT_ENDS_WITH_IGNORE_CASE,
        ChronoStringCompare.STRING_NOT_CONTAINS,
        ChronoStringCompare.STRING_NOT_CONTAINS_IGNORE_CASE,
        ChronoStringCompare.STRING_NOT_EQUALS_IGNORE_CASE,
        ChronoStringCompare.STRING_NOT_MATCHES_REGEX,
        ChronoStringCompare.STRING_NOT_MATCHES_REGEX_IGNORE_CASE
    ));


    private final List<FilterStep<E>> indexableSubsteps = Lists.newArrayList();

    // =====================================================================================================================
    // CONSTRUCTOR
    // =====================================================================================================================

    public ChronoGraphStep(final GraphStep<S, E> originalStep, final List<FilterStep<E>> indexableSteps) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(),
            originalStep.getIds());
        // copy the labels of the original step
        originalStep.getLabels().forEach(this::addLabel);
        // add the sub-steps...
        this.indexableSubsteps.addAll(indexableSteps);
        // ... and copy their labels
        indexableSteps.forEach(subStep -> subStep.getLabels().forEach(this::addLabel));
        // set the result iterator supplier function (i.e. the function that calculates the result of this step)
        this.setIteratorSupplier(this::getResultIterator);
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
    private Iterator<E> getResultIterator() {
        if (Vertex.class.isAssignableFrom(this.returnClass)) {
            return (Iterator<E>) this.getResultVertices();
        } else {
            return (Iterator<E>) this.getResultEdges();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Iterator<Vertex> getResultVertices() {
        ChronoGraph graph = ChronoGraphTraversalUtil.getChronoGraph(this.getTraversal());
        // ensure that we have an open transaction...
        graph.tx().readWrite();
        // ... and retrieve it
        ChronoGraphTransaction tx = ChronoGraphTraversalUtil.getTransaction(this.getTraversal());

        Set<ChronoGraphIndex> cleanIndices = graph.getIndexManagerOnBranch(tx.getBranchName()).getCleanIndicesAtTimestamp(tx.getTimestamp());
        // start the query builder...
        ChronoDBTransaction dbTx = tx.getBackingDBTransaction();
        QueryBuilder queryBuilder = dbTx.find().inKeyspace(ChronoGraphConstants.KEYSPACE_VERTEX);
        // ... and translate our filter steps into a ChronoDB query
        FinalizableQueryBuilder finalizableQueryBuilder = ChronoGraphTraversalUtil.toChronoDBQuery(
            cleanIndices,
            this.indexableSubsteps,
            queryBuilder,
            ChronoGraphTraversalUtil::createIndexKeyForVertexProperty
        );
        Iterator<QualifiedKey> keys = finalizableQueryBuilder.getKeys();
        Set<Vertex> verticesFromIndexQuery = Streams.stream(keys)
            .map(QualifiedKey::getKey)
            .map(id -> tx.getVertexOrNull(id, ElementLoadMode.LAZY))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        // check if any of our predicates is negated.
        // The reason why we have to do this is a slight difference in query semantics
        // between ChronoDB and Gremlin when it comes to NEGATED predicates:
        // - In ChronoDB, a key is returned if its value matches the negated predicate. Note that "null" matches many negated predicates.
        // - In Gremlin, a graph element is returned if it HAS a value AND that value matches the negated predicate.
        // We therefore need to apply a post-processing here, checking that the vertices indeed have the requested property keys.

        if(this.isAnyPredicateNegated(this.indexableSubsteps)){
            Predicate<Vertex> predicate = (Predicate<Vertex>) ChronoGraphTraversalUtil.filterStepsToPredicate(this.indexableSubsteps);
            verticesFromIndexQuery = verticesFromIndexQuery.stream().filter(predicate).collect(Collectors.toSet());
        }

        // consider the transaction context
        GraphTransactionContextInternal context = (GraphTransactionContextInternal) tx.getContext();
        if (!context.isDirty()) {
            // return the index query result directly
            return verticesFromIndexQuery.iterator();
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
            .iterator();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Iterator<Edge> getResultEdges() {
        ChronoGraph graph = ChronoGraphTraversalUtil.getChronoGraph(this.getTraversal());
        // ensure that we have an open transaction...
        graph.tx().readWrite();
        // ... and retrieve it
        ChronoGraphTransaction tx = ChronoGraphTraversalUtil.getTransaction(this.getTraversal());

        Set<ChronoGraphIndex> cleanIndices = graph.getIndexManagerOnBranch(tx.getBranchName()).getCleanIndicesAtTimestamp(tx.getTimestamp());
        // start the query builder...
        ChronoDBTransaction dbTx = tx.getBackingDBTransaction();
        QueryBuilder queryBuilder = dbTx.find().inKeyspace(ChronoGraphConstants.KEYSPACE_EDGE);
        // ... and translate our filter steps into a ChronoDB query
        FinalizableQueryBuilder finalizableQueryBuilder = ChronoGraphTraversalUtil.toChronoDBQuery(
            cleanIndices,
            this.indexableSubsteps,
            queryBuilder,
            ChronoGraphTraversalUtil::createIndexKeyForEdgeProperty
        );
        Set<Edge> edgesFromIndexQuery = Streams.stream(finalizableQueryBuilder.getKeys())
            .map(QualifiedKey::getKey)
            .map(id -> tx.getEdgeOrNull(id, ElementLoadMode.LAZY))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        // check if any of our predicates is negated.
        // The reason why we have to do this is a slight difference in query semantics
        // between ChronoDB and Gremlin when it comes to NEGATED predicates:
        // - In ChronoDB, a key is returned if its value matches the negated predicate. Note that "null" matches many negated predicates.
        // - In Gremlin, a graph element is returned if it HAS a value AND that value matches the negated predicate.
        // We therefore need to apply a post-processing here, checking that the vertices indeed have the requested property keys.

        if(this.isAnyPredicateNegated(this.indexableSubsteps)){
            Predicate<Edge> predicate = (Predicate<Edge>) ChronoGraphTraversalUtil.filterStepsToPredicate(this.indexableSubsteps);
            edgesFromIndexQuery = edgesFromIndexQuery.stream().filter(predicate).collect(Collectors.toSet());
        }

        // consider the transaction context
        GraphTransactionContextInternal context = (GraphTransactionContextInternal) tx.getContext();
        if (!context.isDirty()) {
            // return the index query result directly
            return edgesFromIndexQuery.iterator();
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
            .iterator();
    }

    private boolean isAnyPredicateNegated(final List<FilterStep<E>> indexableSubsteps) {
        for(FilterStep<E> filterStep : indexableSubsteps){
            if(this.isNegated(filterStep)){
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private boolean isNegated(FilterStep<E> filterStep){
        if(filterStep instanceof NotStep){
            // note: we COULD check for double-negations here...
            return true;
        } else if(filterStep instanceof ConnectiveStep){
            ConnectiveStep<E> connectiveStep = (ConnectiveStep<E>)filterStep;
            List<Admin<E, ?>> children = connectiveStep.getLocalChildren();
            for(Admin<E, ?> child : children){
                if(child instanceof FilterStep){
                    if(isNegated((FilterStep<E>)child)){
                        return true;
                    }
                }
            }
        } else if(filterStep instanceof HasStep){
            HasStep<E> hasStep = (HasStep<E>)filterStep;
            for(HasContainer container : hasStep.getHasContainers()){
                BiPredicate<?, ?> biPredicate = container.getBiPredicate();
                if(NEGATED_PREDICATES.contains(biPredicate)){
                    return true;
                }else if(biPredicate instanceof DoubleWithoutCP){
                    return true;
                }else if(biPredicate instanceof LongWithoutCP){
                    return true;
                }else if(biPredicate instanceof StringWithoutCP){
                    return true;
                }
            }
        }
        return false;
    }

}
