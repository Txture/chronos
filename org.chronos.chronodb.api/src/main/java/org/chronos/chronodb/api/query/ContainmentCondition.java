package org.chronos.chronodb.api.query;

/**
 * A {@link ContainmentCondition} is a {@link Condition} on any values, checking for (non-)containment in a known collection.
 *
 * @author martin.haeusler@txture.io -- Initial Contribution and API
 *
 */
public interface ContainmentCondition extends Condition {

    /**
     * Negates this condition.
     *
     * @return The negated condition.
     */
    public ContainmentCondition negate();

}
