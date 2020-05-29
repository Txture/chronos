package org.chronos.chronograph.internal.impl.optimizer.strategy;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphStep;
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ChronoGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

    // =====================================================================================================================
    // SINGLETON IMPLEMENTATION
    // =====================================================================================================================

    private static final ChronoGraphStepStrategy INSTANCE;

    public static ChronoGraphStepStrategy getInstance() {
        return INSTANCE;
    }

    static {
        INSTANCE = new ChronoGraphStepStrategy();
    }

    // =====================================================================================================================
    // CONSTRUCTOR
    // =====================================================================================================================

    /**
     * This constructor is private on purpose.
     *
     * <p>
     * Please use {@link #getInstance()} to retrieve the singleton instance of this class.
     */
    private ChronoGraphStepStrategy() {
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        Set<Class<? extends ProviderOptimizationStrategy>> resultSet = Sets.newHashSet();
        resultSet.add(PredicateNormalizationStrategy.class);
        resultSet.add(ReplaceGremlinPredicateWithChronosPredicateStrategy.class);
        return resultSet;
    }

    // =====================================================================================================================
    // TINKERPOP API
    // =====================================================================================================================

    @Override
    @SuppressWarnings("unchecked")
    public void apply(final Traversal.Admin<?, ?> traversal) {
        ChronoGraph chronoGraph = (ChronoGraph) traversal.getGraph().orElse(null);
        if (chronoGraph == null) {
            // cannot apply traversal strategy...
            return;
        }
        Set<ChronoGraphIndex> cleanIndices = chronoGraph.getIndexManager().getCleanIndices();
        // first of all, get all graph steps in our traversal
        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            // we do not perform any optimization if the original graph step has source IDs
            // (because we do not need the secondary indices in this case).
            Object[] ids = originalGraphStep.getIds();
            if (ids != null && ids.length > 0) {
                return;
            }
            List<Step<?, ?>> indexableSteps = Lists.newArrayList();
            // starting from the initial graph step, check which steps are indexable.
            // We separate the "indexable" check from the "indexed" check, because we can
            // potentially apply a re-ordering of filters here.
            // For example:
            //
            //    g.traversal().V().has("X", 1).has("Y", 2).has("Z", 3).out().has("alpha", 4).toSet()
			//
            // Let's assume that X and Z are indexed, but Y is not. Clearly, we want
            // the index query to be "X == 1 AND Z == 3", and has("Y",2) should be an in-memory filter.
            // To achieve this, we "temporarily" also consider has("Y",2) in order to "step over" it.
			//
			// Also note that the indexable check STOPS at the first step which is non-indexable,
			// therefore in the example above, has("alpha", 4) will never be reached because we stop
			// our search at ".out()".
            Step<?, ?> currentStep = originalGraphStep.getNextStep();
            while (ChronoGraphTraversalUtil.isChronoGraphIndexable(currentStep, false)) {
                if(!indexableSteps.isEmpty() && !indexableSteps.get(indexableSteps.size()-1).getLabels().isEmpty()){
                    // the previous step had a label -> stop collecting!
                    break;
                }
                indexableSteps.add(currentStep);
                currentStep = currentStep.getNextStep();
            }
            if (indexableSteps.isEmpty()) {
                // the initial steps of the traversal are not indexable, nothing we can do here.
                // This happens for example in: "g.traversal().V().out()", because "out()" is non-indexable.
                return;
            }

            // from the list of indexable steps, determine which ones are actually indexed, and remove those from the traversal
            List<Step<?, ?>> indexedSteps = indexableSteps.stream()
                .filter(step -> ChronoGraphTraversalUtil.isCoveredByIndices(step, cleanIndices))
                .collect(Collectors.toList());
            if (indexedSteps.isEmpty()) {
                // we have steps which would theoretically indexable, but we lack
                // the necessary (clean) indices, so there's nothing we can do about this.
                // This happens for example if we have "g.traversal().V().has("type", "A")", but
                // the property "type" has no index, or the index is dirty.
                return;
            }

            // remove the steps which are covered by the index from the traversal and collect them in our root step
            indexedSteps.forEach(traversal::removeStep);
            final ChronoGraphStep<?, ?> chronoGraphStep = new ChronoGraphStep(originalGraphStep, indexedSteps);
			TraversalHelper.replaceStep(originalGraphStep, chronoGraphStep, traversal);
        });
    }


}
