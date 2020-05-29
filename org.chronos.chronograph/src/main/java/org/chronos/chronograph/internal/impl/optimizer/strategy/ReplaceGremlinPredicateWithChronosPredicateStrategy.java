package org.chronos.chronograph.internal.impl.optimizer.strategy;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.chronos.chronograph.api.builder.query.*;
import org.chronos.chronograph.internal.impl.query.ChronoCompare;
import org.chronos.chronograph.internal.impl.query.ChronoStringCompare;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

public class ReplaceGremlinPredicateWithChronosPredicateStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

    private static ReplaceGremlinPredicateWithChronosPredicateStrategy INSTANCE = new ReplaceGremlinPredicateWithChronosPredicateStrategy();

    public static ReplaceGremlinPredicateWithChronosPredicateStrategy getINSTANCE() {
        return INSTANCE;
    }

    @Override
    public void apply(final Admin<?, ?> traversal) {
        List<HasContainerHolder> hasContainerHolders = TraversalHelper.getStepsOfAssignableClassRecursively(HasContainerHolder.class, traversal);
        for (HasContainerHolder hasContainerHolder : hasContainerHolders) {
            for (HasContainer container : Lists.newArrayList(hasContainerHolder.getHasContainers())) {
                HasContainer replacement = this.replacePredicateInContainer(container);
                // this strict reference equality check is on purpose
                if (container != replacement) {
                    hasContainerHolder.removeHasContainer(container);
                    hasContainerHolder.addHasContainer(replacement);
                }
            }
        }
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends ProviderOptimizationStrategy>> resultSet = Sets.newHashSet();
        resultSet.add(PredicateNormalizationStrategy.class);
        return resultSet;
    }

    private HasContainer replacePredicateInContainer(HasContainer container) {
        P<?> originalPredicate = container.getPredicate();
        P<?> newPredicate = this.convertPredicate(originalPredicate);
        // this strict reference equality check is on purpose
        if (originalPredicate == newPredicate) {
            // unchanged
            return container;
        }
        // create a new container with the new predicate
        return new HasContainer(container.getKey(), newPredicate);
    }

    private P<?> convertPredicate(P<?> predicate) {
        if (predicate instanceof AndP) {
            return this.convertAndP((AndP<?>) predicate);
        } else if (predicate instanceof OrP) {
            return this.convertOrP((OrP<?>) predicate);
        } else if (predicate instanceof TextP) {
            return this.convertTextP((TextP) predicate);
        } else {
            return this.convertBasicP(predicate);
        }
    }

    private P<?> convertAndP(AndP<?> predicate) {
        List<? extends P<?>> children = predicate.getPredicates();
        List<P<?>> newChildren = Lists.newArrayList();
        boolean changed = false;
        for (P<?> child : children) {
            P<?> replacement = this.convertPredicate(child);
            if (child != replacement) {
                changed = true;
                newChildren.add(replacement);
            } else {
                newChildren.add(child);
            }
        }
        if (changed) {
            // changed, create a new predicate
            return new AndP(newChildren);
        } else {
            // unchanged, return the old predicate
            return predicate;
        }
    }

    private P<?> convertOrP(OrP<?> predicate) {
        List<? extends P<?>> children = predicate.getPredicates();
        List<P<?>> newChildren = Lists.newArrayList();
        boolean changed = false;
        for (P<?> child : children) {
            P<?> replacement = this.convertPredicate(child);
            if (child != replacement) {
                changed = true;
                newChildren.add(replacement);
            } else {
                newChildren.add(child);
            }
        }
        if (changed) {
            // changed, create a new predicate
            return new OrP(newChildren);
        } else {
            // unchanged, return the old predicate
            return predicate;
        }
    }

    private P<?> convertTextP(TextP predicate) {
        // TextP is always converted into the CP's
        BiPredicate<String, String> biPredicate = predicate.getBiPredicate();
        String value = predicate.getValue();
        if (Text.startingWith.equals(biPredicate)) {
            return CP.startsWith(value);
        } else if (Text.endingWith.equals(biPredicate)) {
            return CP.endsWith(value);
        } else if (Text.notStartingWith.equals(biPredicate)) {
            return CP.notStartsWith(value);
        } else if (Text.notEndingWith.equals(biPredicate)) {
            return CP.notEndsWith(value);
        } else if (Text.containing.equals(biPredicate)) {
            return CP.contains(value);
        } else if (Text.notContaining.equals(biPredicate)) {
            return CP.notContains(value);
        } else {
            throw new IllegalArgumentException("Encountered unknown BiPredicate in TextP: " + biPredicate);
        }
    }

    @SuppressWarnings("unchecked")
    private P<?> convertBasicP(P<?> predicate) {
        BiPredicate<?, ?> biPredicate = predicate.getBiPredicate();
        Object value = predicate.getValue();
        if(biPredicate instanceof ChronoCompare
            || biPredicate instanceof ChronoStringCompare
            || biPredicate instanceof DoubleWithinCP
            || biPredicate instanceof DoubleWithoutCP
            || biPredicate instanceof LongWithinCP
            || biPredicate instanceof LongWithoutCP
            || biPredicate instanceof StringWithinCP
            || biPredicate instanceof StringWithoutCP
            || biPredicate instanceof DoubleEqualsCP
            || biPredicate instanceof DoubleNotEqualsCP){
            return predicate;
        }
        if (Compare.eq.equals(biPredicate)) {
            return CP.cEq(value);
        } else if (Compare.neq.equals(biPredicate)) {
            return CP.cNeq(value);
        } else if (Compare.gt.equals(biPredicate)) {
            return CP.cGt(value);
        } else if (Compare.lt.equals(biPredicate)) {
            return CP.cLt(value);
        } else if (Compare.gte.equals(biPredicate)) {
            return CP.cGte(value);
        } else if (Compare.lte.equals(biPredicate)) {
            return CP.cLte(value);
        } else if (Contains.within.equals(biPredicate)) {
            if (value instanceof Collection) {
                Collection<Object> collection = (Collection<Object>) value;
                return CP.cWithin(collection);
            } else {
                return CP.within(value);
            }
        } else if (Contains.without.equals(biPredicate)) {
            if (value instanceof Collection) {
                Collection<Object> collection = (Collection<Object>) value;
                return CP.cWithout(collection);
            } else {
                return CP.without(value);
            }
        }else {
            throw new IllegalArgumentException("Encountered unknown BiPredicate in basic P: " + biPredicate);
        }
    }

}
