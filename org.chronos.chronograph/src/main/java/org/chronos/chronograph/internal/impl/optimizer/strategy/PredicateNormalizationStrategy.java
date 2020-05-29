package org.chronos.chronograph.internal.impl.optimizer.strategy;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class PredicateNormalizationStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

    // =====================================================================================================================
    // SINGLETON IMPLEMENTATION
    // =====================================================================================================================

    private static final PredicateNormalizationStrategy INSTANCE;

    public static PredicateNormalizationStrategy getInstance() {
        return INSTANCE;
    }

    static {
        INSTANCE = new PredicateNormalizationStrategy();
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
    private PredicateNormalizationStrategy() {
    }

    // =====================================================================================================================
    // TINKERPOP API
    // =====================================================================================================================

    @Override
    @SuppressWarnings("unchecked")
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            Step<?, ?> currentStep = originalGraphStep.getNextStep();
            List<Step<?, ?>> indexableSteps = Lists.newArrayList();
            // note that we MUST NOT perform any optimization here if we
            // have a LABEL present on the step!
            while (ChronoGraphTraversalUtil.isChronoGraphIndexable(currentStep, true) && currentStep.getLabels().isEmpty()) {
                indexableSteps.add(currentStep);
                currentStep = currentStep.getNextStep();
            }
            Map<Step, Step> replaceSteps = ChronoGraphTraversalUtil.normalizeConnectivePredicates(traversal, indexableSteps);
            for(Entry<Step, Step> entry : replaceSteps.entrySet()){
                TraversalHelper.replaceStep(entry.getKey(), entry.getValue(),  traversal);
            }
        });
    }


}
