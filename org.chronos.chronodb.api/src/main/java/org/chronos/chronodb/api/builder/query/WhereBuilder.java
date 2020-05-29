package org.chronos.chronodb.api.builder.query;

import org.chronos.chronodb.internal.impl.query.TextMatchMode;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * This interface is a part of the fluent query API for ChronoDB.
 *
 * <p>
 * It allows to specify a variety of conditions in a "where" clause.
 *
 * <p>
 * Please see the individual methods for examples.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface WhereBuilder {

	// =================================================================================================================
	// STRING OPERATIONS
	// =================================================================================================================

	/**
	 * Adds a text containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").contains("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder contains(String text);

	/**
	 * Adds a case-insensitive text containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").containsIgnoreCase("martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder containsIgnoreCase(String text);

	/**
	 * Adds a text not-containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notContains("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notContains(String text);

	/**
	 * Adds a case-insensitive text not-containment constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notContainsIgnoreCase("martin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notContainsIgnoreCase(String text);

	/**
	 * Adds a text "starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").startsWith("Ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder startsWith(String text);

	/**
	 * Adds a case-insensitive text "starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").startsWithIgnoreCase("ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder startsWithIgnoreCase(String text);

	/**
	 * Adds a text "not starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notStartsWith("Ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notStartsWith(String text);

	/**
	 * Adds a case-insensitive text "not starts with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notStartsWithIgnoreCase("ma").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notStartsWithIgnoreCase(String text);

	/**
	 * Adds a text "ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").endsWith("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder endsWith(String text);

	/**
	 * Adds a case-insensitive text "ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").endsWithIgnoreCase("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder endsWithIgnoreCase(String text);

	/**
	 * Adds a text "not ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notEndsWith("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notEndsWith(String text);

	/**
	 * Adds a case-insensitive text "not ends with" constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notEndsWithIgnoreCase("rtin").getResult();
	 * </pre>
	 *
	 *
	 * @param text
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notEndsWithIgnoreCase(String text);

	/**
	 * Adds a regex constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").matchesRegex("He(ll)*o").getResult();
	 * </pre>
	 *
	 *
	 * @param regex
	 *            The regex to search for. Must not be <code>null</code> or empty. Supports the full range of expressions defined in <code>java.util.regex.</code>{@link Pattern}.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder matchesRegex(String regex);

	/**
	 * Adds a negated regex constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notMatchesRegex("He(ll)*o").getResult();
	 * </pre>
	 *
	 *
	 * @param regex
	 *            The regex to search for. Must not be <code>null</code> or empty. Supports the full range of expressions defined in <code>java.util.regex.</code>{@link Pattern}.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notMatchesRegex(String regex);

	/**
	 * Adds a case-insensitive regex constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").matchesRegexIgnoreCase("He(ll)*o").getResult();
	 * </pre>
	 *
	 *
	 * @param regex
	 *            The regex to search for. Must not be <code>null</code> or empty. Supports the full range of expressions defined in <code>java.util.regex.</code>{@link Pattern}.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder matchesRegexIgnoreCase(String regex);

	/**
	 * Adds a case-insensitive, negated regex constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notMatchesRegexIgnoreCase("He(ll)*o").getResult();
	 * </pre>
	 *
	 *
	 * @param regex
	 *            The regex to search for. Must not be <code>null</code> or empty. Supports the full range of expressions defined in <code>java.util.regex.</code>{@link Pattern}.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notMatchesRegexIgnoreCase(String regex);

	/**
	 * Adds a text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isEqualTo("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isEqualTo(String value);

	/**
	 * Adds a case-insensitive text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isEqualToIgnoreCase("martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isEqualToIgnoreCase(String value);

	/**
	 * Adds a negated text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isNotEqualTo("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isNotEqualTo(String value);

	/**
	 * Adds a case-insensitive negated text equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").isNotEqualToIgnoreCase("Martin").getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The text to search for. Must not be <code>null</code> or empty.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isNotEqualToIgnoreCase(String value);

	// =================================================================================================================
	// LONG OPERATIONS
	// =================================================================================================================

	/**
	 * Adds a long equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("height").isEqualTo(8000).getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isEqualTo(long value);

	/**
	 * Adds a negated long equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isNotEqualTo(8000).getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isNotEqualTo(long value);

	/**
	 * Adds a "greater than" ( <code>&gt</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isGreaterThan(8000).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isGreaterThan(long value);

	/**
	 * Adds a "greater than or equal to" ( <code>&gt=</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isGreaterThanOrEqualTo(8000).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isGreaterThanOrEqualTo(long value);

	/**
	 * Adds a "less than" ( <code>&lt</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isLessThan(8000).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isLessThan(long value);

	/**
	 * Adds a "less than or equal to" ( <code>&lt=</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isLessThanOrEqualTo(8000).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isLessThanOrEqualTo(long value);

	// =================================================================================================================
	// DOUBLE OPERATIONS
	// =================================================================================================================

	/**
	 * Adds a long equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("height").isEqualTo(1.75, 0.01).getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The value to compare against.
	 * @param tolerance
	 *            The allowed tolerance range for equality checks. Will be applied in positive AND negative direction. Must not be negative.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isEqualTo(double value, double tolerance);

	/**
	 * Adds a negated long equality constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isNotEqualTo(1.75, 0.01).getResult();
	 * </pre>
	 *
	 *
	 * @param value
	 *            The value to compare against.
	 * @param tolerance
	 *            The allowed tolerance range for equality checks. Will be applied in positive AND negative direction. Must not be negative.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isNotEqualTo(double value, double tolerance);

	/**
	 * Adds a "greater than" ( <code>&gt</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isGreaterThan(1.75).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isGreaterThan(double value);

	/**
	 * Adds a "greater than or equal to" ( <code>&gt=</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isGreaterThanOrEqualTo(1.75).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isGreaterThanOrEqualTo(double value);

	/**
	 * Adds a "less than" ( <code>&lt</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isLessThan(1.75).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isLessThan(double value);

	/**
	 * Adds a "less than or equal to" ( <code>&lt=</code> ) constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).isLessThanOrEqualTo(1.75).getResult();
	 * </pre>
	 *
	 * @param value
	 *            The value to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder isLessThanOrEqualTo(double value);

	// =================================================================================================================
	// SET OPERATIONS
	// =================================================================================================================

	/**
	 * Adds a contained-in-set-constraint, the strings are compared with equalsIgnoreCase
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").inStringsIgnoreCase(Sets.newHashSet("John", "Jane")).getResult();
	 * </pre>
	 *
	 *
	 * @param values
	 *            The set containing the values to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder inStringsIgnoreCase(Set<String> values);

	/**
	 * Adds a contained-in-set-constraint, the strings are strictly compared with equals
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").inStrings(Sets.newHashSet("John", "Jane")).getResult();
	 * </pre>
	 *
	 *
	 * @param values
	 *            The set containing the values to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder inStrings(Set<String> values);

	/**
	 * Adds a not-contained-in-set-constraint, the strings are compared with equalsIgnoreCase
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notInStringsIgnoreCase(Sets.newHashSet("John", "Jane")).getResult();
	 * </pre>
	 *
	 *
	 * @param values
	 *            The set containing the values to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notInStringsIgnoreCase(Set<String> values);

	/**
	 * Adds a not-contained-in-set-constraint, the strings are strictly compared with equals
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where("name").notInStrings(Sets.newHashSet("John", "Jane")).getResult();
	 * </pre>
	 *
	 *
	 * @param values
	 *            The set containing the values to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notInStrings(Set<String> values);

	/**
	 * Adds a contained-in-set-constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).inLongs(Sets.newHashSet(8000, 4000)).getResult();
	 * </pre>
	 *
	 * @param values
	 *            The values to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder inLongs(Set<Long> values);

	/**
	 * Adds a not-contained-in-set-constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).notInLongs(Sets.newHashSet(8000, 4000)).getResult();
	 * </pre>
	 *
	 * @param values
	 *            The values to compare against.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notInLongs(Set<Long> values);

	/**
	 * Adds a contained-in-set-constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).inDoubles(Sets.newHashSet(8000.3, 4000.5)).getResult();
	 * </pre>
	 *
	 * @param values
	 *            The values to compare against.
	 * @param tolerance
	 * 			  The allowed tolerance range for equality checks. Will be applied in positive AND negative direction. Must not be negative.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder inDoubles(Set<Double> values, double tolerance);

	/**
	 * Adds a not-contained-in-set-constraint.
	 *
	 * <p>
	 * Usage example:
	 *
	 * <pre>
	 * tx.find().where(height).notInDoubles(Sets.newHashSet(8000.3, 4000.5)).getResult();
	 * </pre>
	 *
	 * @param values
	 *            The values to compare against.
	 * @param tolerance
	 * 			  The allowed tolerance range for equality checks. Will be applied in positive AND negative direction. Must not be negative.
	 *
	 * @return The next builder. Never <code>null</code>.
	 */
	public FinalizableQueryBuilder notInDoubles(Set<Double> values, double tolerance);

}
