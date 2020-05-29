package org.chronos.chronograph.api.schema;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.structure.ChronoElement;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An immutable data object representing the result of a {@link ChronoGraphSchemaManager#validate(String, ChronoElement)} call.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface SchemaValidationResult {

    /**
     * Checks if this validation result represents a validation success (i.e. no violations have been reported).
     *
     * @return <code>true</code> if this result represents a success, otherwise <code>false</code>.
     */
    public default boolean isSuccess() {
        return this.getFailedValidators().isEmpty();
    }

    /**
     * Checks if this validation result represents a validation failure (i.e. at least one violation has been reported).
     *
     * @return <code>true</code> if this result represents a failure, otherwise <code>false</code>.
     */
    public default boolean isFailure() {
        return !this.isSuccess();
    }

    /**
     * Returns the total number of validation errors.
     *
     * @return The total number of validation errors. Never negative.
     */
    public int getFailureCount();

    /**
     * Returns the set of validator names which reported a violation.
     *
     * @return An immutable set of validator names which reported a violation. Never <code>null</code>, will be empty in case of {@linkplain #isSuccess() success}.
     */
    public Set<String> getFailedValidators();

    /**
     * Returns the set of all violations, grouped by validator.
     *
     * @return A map. The map key is the validator name, the value is a list of violations. Each violation is a pair containing the offending element,
     * as well as the violation exception. The result map is never <code>null</code>. Validators which reported no violations will not be contained in the keyset.
     */
    public Map<String, List<Pair<Element, Throwable>>> getViolationsByValidators();

    /**
     * Returns the set of validator names which reported a violation on the given {@link Element}.
     *
     * @return An immutable set of validator names which reported a violation on the given element. Never <code>null</code>, will be empty in case of {@linkplain #isSuccess() success}.
     */
    public default Set<String> getFailedValidatorsForElement(Element element) {
        return this.getFailedValidatorExceptionsForElement(element).keySet();
    }

    /**
     * Returns a map from validator name to the encountered validation issue for the given {@link Element}.
     *
     * @return An immutable map from validator name to the error thrown by the validator. Never <code>null</code>, will be empty in case of {@linkplain #isSuccess() success}.
     */
    public Map<String, Throwable> getFailedValidatorExceptionsForElement(Element element);

    /**
     * Compacts this validation result into an error message.
     *
     * @return The error message.
     */
    public String generateErrorMessage();

}
