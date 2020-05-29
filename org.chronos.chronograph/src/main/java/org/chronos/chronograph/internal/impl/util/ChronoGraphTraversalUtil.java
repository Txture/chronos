package org.chronos.chronograph.internal.impl.util;

import static com.google.common.base.Preconditions.*;

import java.util.*;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.*;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep.Connective;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.builder.query.QueryBaseBuilder;
import org.chronos.chronodb.api.builder.query.QueryBuilder;
import org.chronos.chronodb.api.builder.query.WhereBuilder;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronograph.api.builder.query.*;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.impl.index.IndexType;
import org.chronos.chronograph.internal.impl.query.ChronoCompare;
import org.chronos.chronograph.internal.impl.query.ChronoStringCompare;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.util.ReflectionUtils;

public class ChronoGraphTraversalUtil {

    /**
     * Returns the {@link ChronoGraph} on which the given {@link Traversal} is executed.
     *
     * <p>
     * This method assumes that the traversal is indeed executed on a ChronoGraph. If that is not the case, an
     * {@link IllegalArgumentException} is thrown.
     *
     * @param traversal The traversal to get the underlying ChronoGraph for. Must not be <code>null</code>.
     * @return The ChronoGraph on which the given traversal is executed. Never <code>null</code>.
     */
    public static ChronoGraph getChronoGraph(final Traversal<?, ?> traversal) {
        checkNotNull(traversal, "Precondition violation - argument 'traversal' must not be NULL!");
        Optional<Graph> optGraph = TraversalHelper.getRootTraversal(traversal.asAdmin()).getGraph();
        if (optGraph.isPresent() == false) {
            throw new IllegalArgumentException("Traversal is not bound to a graph: " + traversal);
        }
        Graph graph = optGraph.get();
        if (graph instanceof ChronoGraph == false) {
            throw new IllegalArgumentException(
                "Traversal is not bound to a ChronoGraph, but a '" + graph.getClass().getName() + "'!");
        }
        return (ChronoGraph) graph;
    }

    /**
     * Returns the {@link ChronoGraphTransaction} on which the given {@link Traversal} is executed.
     *
     * <p>
     * This method assumes that the traversal is indeed executed on a ChronoGraph. If that is not the case, an
     * {@link IllegalArgumentException} is thrown.
     *
     * @param traversal The traversal to get the underlying transaction for. Must not be <code>null</code>.
     * @return The underlying chrono graph transaction. Never <code>null</code>.
     */
    public static ChronoGraphTransaction getTransaction(final Traversal<?, ?> traversal) {
        checkNotNull(traversal, "Precondition violation - argument 'traversal' must not be NULL!");
        ChronoGraph g = getChronoGraph(traversal);
        return g.tx().getCurrentTransaction();
    }

    public static boolean isCoveredByIndices(Step<?, ?> step, Set<ChronoGraphIndex> indices) {
        checkNotNull(step, "Precondition violation - argument 'step' must not be NULL!");
        checkNotNull(indices, "Precondition violation - argument 'indices' must not be NULL!");
        if (!isChronoGraphIndexable(step, false)) {
            // this step contains non-indexable constructs, so it doesn't matter what indices we have available.
            return false;
        }
        ListMultimap<String, ChronoGraphIndex> indicesByProperty = Multimaps.index(indices, ChronoGraphIndex::getIndexedProperty);
        if (step instanceof HasContainerHolder) {
            List<HasContainer> hasContainers = ((HasContainerHolder) step).getHasContainers();
            return hasContainers.stream().allMatch(has -> {
                // try to find an index on the given property
                List<ChronoGraphIndex> chronoGraphIndices = indicesByProperty.get(has.getKey());
                if (indices.isEmpty()) {
                    return false;
                }
                // check the predicate
                BiPredicate<?, ?> biPredicate = has.getBiPredicate();
                if (biPredicate == Compare.eq
                    || biPredicate == ChronoCompare.EQ
                    || biPredicate == Compare.neq
                    || biPredicate == ChronoCompare.NEQ
                    || biPredicate instanceof Contains
                    || biPredicate == ChronoCompare.WITHIN
                    || biPredicate == ChronoCompare.WITHOUT
                ) {
                    // all indices support (in)equality, within and without, just assert that we have an index at all
                    return !chronoGraphIndices.isEmpty();
                } else if (biPredicate == Compare.gt
                    || biPredicate == ChronoCompare.GT
                    || biPredicate == Compare.gte
                    || biPredicate == ChronoCompare.GTE
                    || biPredicate == Compare.lt
                    || biPredicate == ChronoCompare.LT
                    || biPredicate == Compare.lte
                    || biPredicate == ChronoCompare.LTE
                ) {
                    // only numeric indices support those comparison operators
                    return hasIndexerOfType(chronoGraphIndices, IndexType.LONG, IndexType.DOUBLE);
                } else if (biPredicate instanceof DoubleEqualsCP
                    || biPredicate instanceof DoubleNotEqualsCP
                    || biPredicate instanceof DoubleWithinCP
                    || biPredicate instanceof DoubleWithoutCP
                ) {
                    return hasIndexerOfType(chronoGraphIndices, IndexType.DOUBLE);
                } else if (biPredicate instanceof StringWithinCP || biPredicate instanceof StringWithoutCP) {
                    return hasIndexerOfType(chronoGraphIndices, IndexType.STRING);
                } else if (biPredicate instanceof LongWithinCP || biPredicate instanceof LongWithoutCP) {
                    return hasIndexerOfType(chronoGraphIndices, IndexType.LONG);
                } else if (biPredicate instanceof ChronoStringCompare) {
                    // only text indices support those comparison operators
                    return hasIndexerOfType(chronoGraphIndices, IndexType.STRING);
                } else {
                    // unknown predicate...?
                    return false;
                }
            });
        } else if (step instanceof ConnectiveStep) {
            // this case unifies "and" & "or" steps in gremlin (common superclass)
            ConnectiveStep<?> connectiveStep = (ConnectiveStep<?>) step;
            List<? extends Admin<?, ?>> localChildren = connectiveStep.getLocalChildren();
            for (Admin<?, ?> childTraversal : localChildren) {
                for (Step<?, ?> childStep : childTraversal.getSteps()) {
                    if (!isCoveredByIndices(childStep, indices)) {
                        return false;
                    }
                }
            }
            return true;
        } else if (step instanceof NotStep) {
            NotStep<?> notStep = (NotStep<?>) step;
            List<? extends Admin<?, ?>> localChildren = notStep.getLocalChildren();
            for (Admin<?, ?> childTraversal : localChildren) {
                for (Step<?, ?> childStep : childTraversal.getSteps()) {
                    if (!isCoveredByIndices(childStep, indices)) {
                        return false;
                    }
                }
            }
            return true;
        } else {
            // unknown step?
            return false;
        }
    }

    private static boolean hasIndexerOfType(Collection<ChronoGraphIndex> indices, IndexType... indexTypes) {
        checkNotNull(indices, "Precondition violation - argument 'indices' must not be NULL!");
        checkNotNull(indexTypes, "Precondition violation - argument 'indexTypes' must not be NULL!");
        for (ChronoGraphIndex index : indices) {
            for (IndexType indexType : indexTypes) {
                if (index.getIndexType().equals(indexType)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if the given step can be answered by ChronoGraph indices.
     *
     * @param step              The step to check. Will return <code>false</code> if <code>null</code> is used.
     * @param allowAnyPredicate Whether any predicate (<code>true</code>) is allowed or only predicates supported by ChronoGraph are allowed (<code>false</code>)
     * @return <code>true</code> if the given step can be answered by ChronoGraph indices, or <code>false</code> if not.
     */
    public static boolean isChronoGraphIndexable(Step<?, ?> step, boolean allowAnyPredicate) {
        if (step == null) {
            return false;
        }
        if (step instanceof HasContainerHolder) {
            // we've got a "has(x,y)" step, check the condition
            List<HasContainer> hasContainers = ((HasContainerHolder) step).getHasContainers();
            for (HasContainer hasContainer : hasContainers) {
                if (!allowAnyPredicate && !isChronoGraphIndexablePredicate(hasContainer.getPredicate())) {
                    return false;
                }
            }
            return true;
        } else if (step instanceof ConnectiveStep) {
            // this case unifies "and" & "or" steps in gremlin (common superclass)
            ConnectiveStep<?> connectiveStep = (ConnectiveStep<?>) step;
            List<? extends Admin<?, ?>> localChildren = connectiveStep.getLocalChildren();
            for (Admin<?, ?> childTraversal : localChildren) {
                for (Step<?, ?> childStep : childTraversal.getSteps()) {
                    if (!isChronoGraphIndexable(childStep, allowAnyPredicate)) {
                        return false;
                    }
                }
            }
            return true;
        } else if (step instanceof NotStep) {
            NotStep<?> notStep = (NotStep<?>) step;
            List<? extends Admin<?, ?>> localChildren = notStep.getLocalChildren();
            for (Admin<?, ?> childTraversal : localChildren) {
                for (Step<?, ?> childStep : childTraversal.getSteps()) {
                    if (!isChronoGraphIndexable(childStep, allowAnyPredicate)) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    public static boolean isChronoGraphIndexablePredicate(Predicate<?> predicate) {
        if (predicate instanceof CP) {
            // we can always deal with our own predicates, since we define them
            // ourselves.
            return true;
        }
        if (predicate instanceof ConnectiveP) {
            ConnectiveP<?> connective = (ConnectiveP<?>) predicate;
            List<? extends P<?>> subPredicates = connective.getPredicates();
            for (P<?> subPredicate : subPredicates) {
                if (!isChronoGraphIndexablePredicate(subPredicate)) {
                    return false;
                }
            }
            return true;
        }
        if (predicate instanceof P) {
            P<?> p = (P<?>) predicate;
            if (p.getBiPredicate() instanceof Compare) {
                // eq, neq, gt, lt, leq, geq
                return true;
            }
            if (p.getBiPredicate() instanceof Contains) {
                // within, without
                return true;
            }
            if (p.getBiPredicate() instanceof Text) {
                // (native gremlin) starts with, ends with, ...
                return true;
            }
            // note that AndP and OrP are EXCLUDED here! We normalize them in a strategy.
            // all other bi-predicates are unknown...
            return false;
        }
        // all other predicates are unknown to the indexer
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T extends Element> FinalizableQueryBuilder toChronoDBQuery(Set<ChronoGraphIndex> indices, List<FilterStep<T>> indexableSteps, QueryBaseBuilder<?> queryBuilder, Function<String, String> createIndexPropertyKey) {
        QueryBaseBuilder<?> currentBuilder = queryBuilder;
        currentBuilder = currentBuilder.begin();
        boolean firstStep = true;
        for (FilterStep<T> step : indexableSteps) {
            if (!firstStep) {
                // filter steps within a linear sequence are always implicitly AND connected in gremlin
                currentBuilder = applyConnective(currentBuilder, Connective.AND);
            }
            firstStep = false;
            if (step instanceof AndStep) {
                currentBuilder = currentBuilder.begin();
                AndStep<T> andStep = (AndStep<T>) step;
                List<Admin<T, ?>> childTraversals = andStep.getLocalChildren();
                boolean first = true;
                for (Admin<T, ?> childTraversal : childTraversals) {
                    if (!first) {
                        currentBuilder = applyConnective(currentBuilder, Connective.AND);
                    }
                    first = false;
                    List<FilterStep<T>> steps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                    currentBuilder = toChronoDBQuery(indices, steps, currentBuilder, createIndexPropertyKey);
                }
                currentBuilder = currentBuilder.end();
            } else if (step instanceof OrStep) {
                currentBuilder = currentBuilder.begin();
                OrStep<T> orStep = (OrStep<T>) step;
                List<Admin<T, ?>> childTraversals = orStep.getLocalChildren();
                boolean first = true;
                for (Admin<T, ?> childTraversal : childTraversals) {
                    if (!first) {
                        currentBuilder = applyConnective(currentBuilder, Connective.OR);
                    }
                    first = false;
                    List<FilterStep<T>> steps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                    currentBuilder = toChronoDBQuery(indices, steps, currentBuilder, createIndexPropertyKey);
                }
                currentBuilder = currentBuilder.end();
            } else if (step instanceof NotStep) {
                currentBuilder = currentBuilder.not().begin();
                NotStep<T> notStep = (NotStep<T>) step;
                List<Admin<T, ?>> childTraversals = notStep.getLocalChildren();
                boolean first = true;
                for (Admin<T, ?> childTraversal : childTraversals) {
                    if (!first) {
                        currentBuilder = applyConnective(currentBuilder, Connective.AND);
                    }
                    first = false;
                    List<FilterStep<T>> steps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                    currentBuilder = toChronoDBQuery(indices, steps, currentBuilder, createIndexPropertyKey);
                }
                currentBuilder = currentBuilder.end();
            } else if (step instanceof HasContainerHolder) {
                List<HasContainer> hasContainers = ((HasContainerHolder) step).getHasContainers();
                boolean first = true;
                for (HasContainer hasContainer : hasContainers) {
                    if (!first) {
                        currentBuilder = applyConnective(currentBuilder, Connective.AND);
                    }
                    first = false;
                    Set<IndexType> indexTypes = indices.stream()
                        .filter(index -> index.getIndexedProperty().equals(hasContainer.getKey()))
                        .map(ChronoGraphIndex::getIndexType)
                        .collect(Collectors.toSet());

                    String gremlinPropertyName = hasContainer.getKey();
                    String indexPropertyKey = createIndexPropertyKey.apply(gremlinPropertyName);
                    // String indexPropertyKey = ChronoGraphConstants.INDEX_PREFIX_VERTEX + gremlinPropertyName;


                    WhereBuilder whereBuilder = ((QueryBuilder) currentBuilder).where(indexPropertyKey);
                    currentBuilder = applyWhereClause(indexTypes, whereBuilder, hasContainer.getBiPredicate(), hasContainer.getValue());
                }
            } else {
                throw new IllegalStateException("Unexpected step for index query: " + step.getClass().getName());
            }
        }
        currentBuilder = currentBuilder.end();
        return (FinalizableQueryBuilder) currentBuilder;
    }

    private static QueryBaseBuilder<?> applyConnective(QueryBaseBuilder<?> currentBuilder, final Connective connective) {
        switch (connective) {
            case OR:
                return ((FinalizableQueryBuilder) currentBuilder).or();
            case AND:
                return ((FinalizableQueryBuilder) currentBuilder).and();
            default:
                throw new UnknownEnumLiteralException(connective);
        }
    }

    private static FinalizableQueryBuilder applyWhereClause(Set<IndexType> indexTypes, WhereBuilder whereBuilder, BiPredicate<?, ?> biPredicate, Object value) {
        if (biPredicate.equals(Compare.eq) || biPredicate.equals(ChronoCompare.EQ)) {
            if (indexTypes.contains(IndexType.LONG) && ReflectionUtils.isLongCompatible(value)) {
                return whereBuilder.isEqualTo(ReflectionUtils.asLong(value));
            } else if (indexTypes.contains(IndexType.DOUBLE) && ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isEqualTo(ReflectionUtils.asDouble(value), 0.0);
            } else if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.isEqualTo((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(Compare.neq) || biPredicate.equals(ChronoCompare.NEQ)) {
            if (indexTypes.contains(IndexType.LONG) && ReflectionUtils.isLongCompatible(value)) {
                return whereBuilder.isNotEqualTo(ReflectionUtils.asLong(value));
            } else if (indexTypes.contains(IndexType.DOUBLE) && ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isNotEqualTo(ReflectionUtils.asDouble(value), 0.0);
            } else if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.isNotEqualTo((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(Compare.lt) || biPredicate.equals(ChronoCompare.LT)) {
            if (indexTypes.contains(IndexType.LONG) && ReflectionUtils.isLongCompatible(value)) {
                return whereBuilder.isLessThan(ReflectionUtils.asLong(value));
            } else if (indexTypes.contains(IndexType.DOUBLE) && ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isLessThan(ReflectionUtils.asDouble(value));
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(Compare.lte) || biPredicate.equals(ChronoCompare.LTE)) {
            if (indexTypes.contains(IndexType.LONG) && ReflectionUtils.isLongCompatible(value)) {
                return whereBuilder.isLessThanOrEqualTo(ReflectionUtils.asLong(value));
            } else if (ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isLessThanOrEqualTo(ReflectionUtils.asDouble(value));
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(Compare.gt) || biPredicate.equals(ChronoCompare.GT)) {
            if (indexTypes.contains(IndexType.LONG) && ReflectionUtils.isLongCompatible(value)) {
                return whereBuilder.isGreaterThan(ReflectionUtils.asLong(value));
            } else if (indexTypes.contains(IndexType.DOUBLE) && ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isGreaterThan(ReflectionUtils.asDouble(value));
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(Compare.gte) || biPredicate.equals(ChronoCompare.GTE)) {
            if (indexTypes.contains(IndexType.LONG) && ReflectionUtils.isLongCompatible(value)) {
                return whereBuilder.isGreaterThanOrEqualTo(ReflectionUtils.asLong(value));
            } else if (indexTypes.contains(IndexType.DOUBLE) && ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isGreaterThanOrEqualTo(ReflectionUtils.asDouble(value));
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(Contains.within) || biPredicate.equals(ChronoCompare.WITHIN)) {
            if (value instanceof Collection) {
                Collection<?> collection = (Collection<?>) value;
                if (indexTypes.contains(IndexType.STRING) && collection.stream().allMatch(String.class::isInstance)) {
                    Set<String> strings = collection.stream().map(String.class::cast).collect(Collectors.toSet());
                    return whereBuilder.inStrings(strings);
                } else if (indexTypes.contains(IndexType.LONG) && collection.stream().allMatch(ReflectionUtils::isLongCompatible)) {
                    Set<Long> longs = collection.stream().map(ReflectionUtils::asLong).collect(Collectors.toSet());
                    return whereBuilder.inLongs(longs);
                } else if (indexTypes.contains(IndexType.DOUBLE) && collection.stream().allMatch(ReflectionUtils::isDoubleCompatible)) {
                    Set<Double> doubles = collection.stream().map(ReflectionUtils::asDouble).collect(Collectors.toSet());
                    return whereBuilder.inDoubles(doubles, 0.0);
                } else {
                    throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
                }
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(Contains.without) || biPredicate.equals(ChronoCompare.WITHOUT)) {
            if (value instanceof Collection) {
                Collection<?> collection = (Collection<?>) value;
                if (indexTypes.contains(IndexType.STRING) && collection.stream().allMatch(String.class::isInstance)) {
                    Set<String> strings = collection.stream().map(String.class::cast).collect(Collectors.toSet());
                    return whereBuilder.notInStrings(strings);
                } else if (indexTypes.contains(IndexType.LONG) && collection.stream().allMatch(ReflectionUtils::isLongCompatible)) {
                    Set<Long> longs = collection.stream().map(ReflectionUtils::asLong).collect(Collectors.toSet());
                    return whereBuilder.notInLongs(longs);
                } else if (indexTypes.contains(IndexType.DOUBLE) && collection.stream().allMatch(ReflectionUtils::isDoubleCompatible)) {
                    Set<Double> doubles = collection.stream().map(ReflectionUtils::asDouble).collect(Collectors.toSet());
                    return whereBuilder.notInDoubles(doubles, 0.0);
                } else {
                    throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
                }
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate instanceof DoubleWithinCP) {
            if (indexTypes.contains(IndexType.DOUBLE) && value instanceof Collection) {
                Collection<?> collection = (Collection<?>) value;
                if (collection.stream().allMatch(ReflectionUtils::isDoubleCompatible)) {
                    Set<Double> doubles = collection.stream().map(ReflectionUtils::asDouble).collect(Collectors.toSet());
                    return whereBuilder.notInDoubles(doubles, ((DoubleWithinCP) biPredicate).getTolerance());
                } else {
                    throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
                }
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate instanceof DoubleWithoutCP) {
            if (indexTypes.contains(IndexType.DOUBLE) && value instanceof Collection) {
                Collection<?> collection = (Collection<?>) value;
                if (collection.stream().allMatch(ReflectionUtils::isDoubleCompatible)) {
                    Set<Double> doubles = collection.stream().map(ReflectionUtils::asDouble).collect(Collectors.toSet());
                    return whereBuilder.notInDoubles(doubles, ((DoubleWithoutCP) biPredicate).getTolerance());
                } else {
                    throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
                }
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_EQUALS_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.isEqualToIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_EQUALS_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.isNotEqualToIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_CONTAINS) || biPredicate.equals(Text.containing)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.contains((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_CONTAINS_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.containsIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_CONTAINS) || biPredicate.equals(Text.notContaining)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notContains((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_CONTAINS_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notContainsIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_STARTS_WITH) || biPredicate.equals(Text.startingWith)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.startsWith((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_STARTS_WITH_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.startsWithIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_STARTS_WITH) || biPredicate.equals(Text.notStartingWith)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notStartsWith((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_STARTS_WITH_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notStartsWithIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_ENDS_WITH) || biPredicate.equals(Text.endingWith)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.endsWith((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_ENDS_WITH_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.endsWithIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_ENDS_WITH) || biPredicate.equals(Text.notEndingWith)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notEndsWith((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_ENDS_WITH_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notEndsWithIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_MATCHES_REGEX)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.matchesRegex((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_MATCHES_REGEX_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.matchesRegexIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_MATCHES_REGEX)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notMatchesRegex((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate.equals(ChronoStringCompare.STRING_NOT_MATCHES_REGEX_IGNORE_CASE)) {
            if (indexTypes.contains(IndexType.STRING) && value instanceof String) {
                return whereBuilder.notMatchesRegexIgnoreCase((String) value);
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate instanceof DoubleEqualsCP) {
            if (indexTypes.contains(IndexType.DOUBLE) && ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isEqualTo(ReflectionUtils.asDouble(value), ((DoubleEqualsCP) biPredicate).getTolerance());
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else if (biPredicate instanceof DoubleNotEqualsCP) {
            if (indexTypes.contains(IndexType.DOUBLE) && ReflectionUtils.isDoubleCompatible(value)) {
                return whereBuilder.isEqualTo(ReflectionUtils.asDouble(value), ((DoubleNotEqualsCP) biPredicate).getTolerance());
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        }else if(biPredicate instanceof StringWithinCP) {
            if (value instanceof Collection) {
                Collection<?> collection = (Collection<?>) value;
                if (indexTypes.contains(IndexType.STRING) && collection.stream().allMatch(String.class::isInstance)) {
                    Set<String> strings = collection.stream().map(String.class::cast).collect(Collectors.toSet());
                    TextMatchMode matchMode = ((StringWithinCP)biPredicate).getMatchMode();
                    switch(matchMode){
                        case STRICT:
                            return whereBuilder.inStrings(strings);
                        case CASE_INSENSITIVE:
                            return whereBuilder.inStringsIgnoreCase(strings);
                        default:
                            throw new UnknownEnumLiteralException(matchMode);
                    }
                } else {
                    throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
                }
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        }else if(biPredicate instanceof StringWithoutCP){
            if (value instanceof Collection) {
                Collection<?> collection = (Collection<?>) value;
                if (indexTypes.contains(IndexType.STRING) && collection.stream().allMatch(String.class::isInstance)) {
                    Set<String> strings = collection.stream().map(String.class::cast).collect(Collectors.toSet());
                    TextMatchMode matchMode = ((StringWithoutCP)biPredicate).getMatchMode();
                    switch(matchMode){
                        case STRICT:
                            return whereBuilder.notInStrings(strings);
                        case CASE_INSENSITIVE:
                            return whereBuilder.notInStringsIgnoreCase(strings);
                        default:
                            throw new UnknownEnumLiteralException(matchMode);
                    }
                } else {
                    throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
                }
            } else {
                throw createInvalidIndexAccessException(indexTypes, biPredicate, value);
            }
        } else {
            throw new IllegalArgumentException("Unknown predicate '" + biPredicate + "'!");
        }
    }

    private static RuntimeException createInvalidIndexAccessException(final Set<IndexType> indexTypes, final BiPredicate<?, ?> biPredicate, final Object value) {
        throw new IllegalArgumentException("Cannot construct filter with predicate '" + biPredicate + "' and value '" + value + "' of type " + value.getClass().getName() + " for index type(s) " + indexTypes + "!");
    }

    public static <T extends Element> Predicate<T> filterStepsToPredicate(List<FilterStep<T>> filterSteps) {
        // our base predicate accepts everything
        Predicate<T> currentPredicate = (e) -> true;
        for (FilterStep<T> filterStep : filterSteps) {
            // in gremlin, successive filter steps are implicitly AND connected
            currentPredicate = currentPredicate.and(filterStepToPredicate(filterStep));
        }
        return currentPredicate;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T extends Element> Predicate<T> filterStepToPredicate(FilterStep<T> filterStep) {
        if (filterStep instanceof HasContainerHolder) {
            List<HasContainer> hasContainers = ((HasContainerHolder) filterStep).getHasContainers();
            return (e) -> HasContainer.testAll(e, hasContainers);
        } else if (filterStep instanceof AndStep) {
            AndStep<T> andStep = (AndStep<T>) filterStep;
            List<Admin<T, ?>> childTraversals = andStep.getLocalChildren();
            Predicate<T> currentPredicate = (e) -> true;
            for (Admin<T, ?> childTraversal : childTraversals) {
                List<FilterStep<T>> childSteps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                currentPredicate = currentPredicate.and(filterStepsToPredicate(childSteps));
            }
            return currentPredicate;
        } else if (filterStep instanceof OrStep) {
            OrStep<T> orStep = (OrStep<T>) filterStep;
            List<Admin<T, ?>> childTraversals = orStep.getLocalChildren();
            // we use FALSE as the default value for this predicate because what follows
            // is a sequence of OR-connected predicates.
            Predicate<T> currentPredicate = (e) -> false;
            for (Admin<T, ?> childTraversal : childTraversals) {
                List<FilterStep<T>> childSteps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                currentPredicate = currentPredicate.or(filterStepsToPredicate(childSteps));
            }
            return currentPredicate;
        } else if (filterStep instanceof NotStep) {
            NotStep<T> notStep = (NotStep<T>) filterStep;
            List<Admin<T, ?>> childTraversals = notStep.getLocalChildren();
            Predicate<T> currentPredicate = (e) -> true;
            for (Admin<T, ?> childTraversal : childTraversals) {
                List<FilterStep<T>> childSteps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                currentPredicate = currentPredicate.and(filterStepsToPredicate(childSteps));
            }
            return currentPredicate.negate();
        } else {
            throw new IllegalArgumentException("Unknown filter step: " + filterStep);
        }
    }

    public static <T extends Element> Set<String> getHasPropertyKeys(List<FilterStep<T>> filterSteps) {
        return filterSteps.stream().flatMap(step -> getHasPropertyKeys(step).stream()).collect(Collectors.toSet());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T extends Element> Set<String> getHasPropertyKeys(FilterStep<T> filterStep) {
        if (filterStep instanceof HasContainerHolder) {
            List<HasContainer> hasContainers = ((HasContainerHolder) filterStep).getHasContainers();
            return hasContainers.stream().map(HasContainer::getKey).collect(Collectors.toSet());
        } else if (filterStep instanceof AndStep) {
            AndStep<T> andStep = (AndStep<T>) filterStep;
            List<Admin<T, ?>> childTraversals = andStep.getLocalChildren();
            Set<String> resultSet = Sets.newHashSet();
            for (Admin<T, ?> childTraversal : childTraversals) {
                List<FilterStep<T>> childSteps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                resultSet.addAll(getHasPropertyKeys(childSteps));
            }
            return resultSet;
        } else if (filterStep instanceof OrStep) {
            OrStep<T> orStep = (OrStep<T>) filterStep;
            List<Admin<T, ?>> childTraversals = orStep.getLocalChildren();
            // we use FALSE as the default value for this predicate because what follows
            // is a sequence of OR-connected predicates.
            Set<String> resultSet = Sets.newHashSet();
            for (Admin<T, ?> childTraversal : childTraversals) {
                List<FilterStep<T>> childSteps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                resultSet.addAll(getHasPropertyKeys(childSteps));
            }
            return resultSet;
        } else if (filterStep instanceof NotStep) {
            NotStep<T> notStep = (NotStep<T>) filterStep;
            List<Admin<T, ?>> childTraversals = notStep.getLocalChildren();
            Set<String> resultSet = Sets.newHashSet();
            for (Admin<T, ?> childTraversal : childTraversals) {
                List<FilterStep<T>> childSteps = (List<FilterStep<T>>) (List) childTraversal.getSteps();
                resultSet.addAll(getHasPropertyKeys(childSteps));
            }
            return resultSet;
        } else {
            throw new IllegalArgumentException("Unknown filter step: " + filterStep);
        }
    }

    public static String createIndexKeyForVertexProperty(String property) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        return ChronoGraphConstants.INDEX_PREFIX_VERTEX + property;
    }

    public static String createIndexKeyForEdgeProperty(String property) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        return ChronoGraphConstants.INDEX_PREFIX_EDGE + property;
    }

    /**
     * Normalizes connective (AND & OR) predicates into AND/OR gremlin queries.
     *
     * <p>
     * Example Input:
     *
     * <pre>
     * has("name", P.eq("John").or("Jane").or("Jack").and("Sarah"))
     * </pre>
     *
     * Example Output:
     * <pre>
     * __.and(
     *      __.or(
     *           __.has("name", "Jack"),
     *           __.or(
     *                __.has("name", "Jane"),
     *                __.has("name", "John")
     *           )
     *      ),
     *      __.has("name", "Sarah")
     * )
     * </pre>
     * </p>
     *
     * @param traversal The original traversal to normalize. Must not be <code>null</code>.
     * @param steps     The steps of the traversal to analyze. Must not be <code>null</code>.
     * @return A map containing the required replacements to perform on the traversal in order to normalize it. Steps which do not occur as keys are already normalized and can stay the same.
     */
    @SuppressWarnings("rawtypes")
    public static Map<Step, Step> normalizeConnectivePredicates(Traversal.Admin traversal, final List<Step<?, ?>> steps) {
        checkNotNull(traversal, "Precondition violation - argument 'traversal' must not be NULL!");
        checkNotNull(steps, "Precondition violation - argument 'steps' must not be NULL!");
        Map<Step, Step> topLevelReplacements = Maps.newHashMap();
        for (Step<?, ?> topLevelStep : steps) {
            Step replacementStep = normalizeConnectivePredicates(traversal, topLevelStep);
            if (replacementStep != null) {
                topLevelReplacements.put(topLevelStep, replacementStep);
            }
        }
        return topLevelReplacements;
    }

    @SuppressWarnings({"unchecked"})
    private static Step normalizeConnectivePredicates(Traversal.Admin traversal, Step topLevelStep) {
        if (topLevelStep instanceof HasContainerHolder) {
            List<HasContainer> hasContainers = ((HasContainerHolder) topLevelStep).getHasContainers();
            List<FilterStep<Element>> filterSteps = hasContainers.stream()
                .map(has -> normalizePredicate(traversal, has))
                .collect(Collectors.toList());
            if (filterSteps.size() == 1) {
                // we only have one step, keep it as-is
                return Iterables.getOnlyElement(filterSteps);
            }
            // we have multiple steps, create a wrapping AND step
            Traversal[] subTraversals = filterSteps.stream()
                .map(ChronoGraphTraversalUtil::createTraversalForStep)
                .toArray(Traversal[]::new);
            // has-containers are ALWAYS implicitly AND-connected
            return new AndStep(traversal, subTraversals);
        } else if (topLevelStep instanceof AndStep) {
            AndStep<?> andStep = (AndStep<?>) topLevelStep;
            List<? extends Admin<?, ?>> childTraversals = andStep.getLocalChildren();
            Traversal[] transformedChildTraversals = childTraversals.stream().map(childTraversal -> {
                List<FilterStep<?>> childSteps = (List<FilterStep<?>>) (List) childTraversal.getSteps();
                List<Step<?, ?>> transformedChildSteps = childSteps.stream()
                    .map(childStep -> (Step<?, ?>) normalizeConnectivePredicates(traversal, childStep))
                    .collect(Collectors.toList());
                return createTraversalForSteps(transformedChildSteps);
            }).toArray(Traversal[]::new);
            return new AndStep(traversal, transformedChildTraversals);
        } else if (topLevelStep instanceof OrStep) {
            OrStep<?> andStep = (OrStep<?>) topLevelStep;
            List<? extends Admin<?, ?>> childTraversals = andStep.getLocalChildren();
            Traversal[] transformedChildTraversals = childTraversals.stream().map(childTraversal -> {
                List<FilterStep<?>> childSteps = (List<FilterStep<?>>) (List) childTraversal.getSteps();
                List<Step<?, ?>> transformedChildSteps = childSteps.stream()
                    .map(childStep -> (Step<?, ?>) normalizeConnectivePredicates(traversal, childStep))
                    .collect(Collectors.toList());
                return createTraversalForSteps(transformedChildSteps);
            }).toArray(Traversal[]::new);
            return new OrStep(traversal, transformedChildTraversals);
        } else if (topLevelStep instanceof NotStep) {
            NotStep<?> notStep = (NotStep<?>) topLevelStep;
            List<? extends Admin<?, ?>> childTraversals = notStep.getLocalChildren();
            // NOT may only have a single child traversal!
            Admin<?, ?> childTraversal = Iterables.getOnlyElement(childTraversals);
            List<FilterStep<?>> childSteps = (List<FilterStep<?>>) (List) childTraversal.getSteps();
            List<Step<?, ?>> transformedChildSteps = childSteps.stream()
                .map(childStep -> (Step<?, ?>) normalizeConnectivePredicates(traversal, childStep))
                .collect(Collectors.toList());
            GraphTraversal.Admin transformedChildTraversal = createTraversalForSteps(transformedChildSteps);
            return new NotStep(traversal, transformedChildTraversal);
        } else {
            // do not modify anything else
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V extends Element> FilterStep<V> normalizePredicate(Traversal.Admin traversal, HasContainer hasContainer) {
        P predicate = hasContainer.getPredicate();
        return normalizePredicate(traversal, hasContainer.getKey(), (P<V>) predicate);
    }

    @SuppressWarnings("unchecked")
    private static <V extends Element> FilterStep<V> normalizePredicate(Traversal.Admin traversal, String propertyKey, P<V> predicate) {
        if (predicate instanceof AndP) {
            // replace and-predicate by traversal and steps
            AndP<V> orPredicate = (AndP<V>) predicate;
            List<P<V>> subPredicates = orPredicate.getPredicates();
            Traversal[] transformedChildTraversals = subPredicates.stream()
                .map(p -> normalizePredicate(traversal, propertyKey, p))
                .map(ChronoGraphTraversalUtil::createTraversalForStep)
                .toArray(Traversal[]::new);
            return new AndStep(traversal, transformedChildTraversals);
        } else if (predicate instanceof OrP) {
            // replace or-predicate by traversal or steps
            OrP<V> orPredicate = (OrP<V>) predicate;
            List<P<V>> subPredicates = orPredicate.getPredicates();
            return new OrStep(traversal, subPredicates.stream()
                .map(p -> (Step<?, ?>) normalizePredicate(traversal, propertyKey, p))
                .map(ChronoGraphTraversalUtil::createTraversalForStep)
                .toArray(Traversal[]::new)
            );
        } else {
            // in all other cases, we keep the predicate intact
            return new HasStep<>(traversal, new HasContainer(propertyKey, predicate));
        }
    }

    private static <S, E> GraphTraversal.Admin<?, ?> createTraversalForStep(final Step<S, E> step) {
        return __.start().asAdmin().addStep(step);
    }

    private static GraphTraversal.Admin<?, ?> createTraversalForSteps(final List<Step<?, ?>> steps) {
        GraphTraversal.Admin<Object, Object> traversal = __.start().asAdmin();
        for (Step<?, ?> step : steps) {
            traversal.addStep(step);
        }
        return traversal;
    }

}
