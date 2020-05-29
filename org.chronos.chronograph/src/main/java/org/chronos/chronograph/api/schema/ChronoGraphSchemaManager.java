package org.chronos.chronograph.api.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.chronos.chronograph.api.exceptions.ChronoGraphSchemaViolationException;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;

import java.util.Set;

/**
 * A manager for schema constraints on a {@link ChronoGraph} instance.
 *
 * <p>
 * Validation constraints are expressed as groovy scripts.
 * </p>
 *
 * <h1>Scripting API</h1>
 *
 * A validator script is a Groovy script, which is compiled with <b>static type checking</b>
 * (i.e. dynamic features of Groovy are unavailable). The script has access to the
 * following global variables:
 *
 * <ul>
 * <li><b><code>element</code></b>: The graph element ({@link ChronoVertex} or {@link ChronoEdge} to validate. Never <code>null</code>. Navigation to neighboring elements is permitted, but be aware of performance implications in case of excessive querying.</li>
 * <li><b><code>branch</code></b>: The name of the branch on which the validation occurs, as a string. Never <code>null</code>.</li>
 * </ul>
 *
 * <h2>Validator Results</h2>
 * In case that the validator script encounters a schema violation, it should throw a {@link ChronoGraphSchemaViolationException}. If
 * the script exits without throwing any exception, the graph is assumed to conform to the schema. If an exception other than
 * {@link ChronoGraphSchemaViolationException} is thrown, the validator itself is invalid. In this case, a warning will be printed
 * to the console, and the assumption will be made that the graph violates the schema (since its structure caused a validator to fail).
 *
 * <h2>Assumptions about the Validator Scripts</h2>
 * The following basic assumptions are made (but not enforced) about the validator script:
 * <ul>
 * <li><b>Idempotence</b>: Given the same input graph, it will always and consistently produce the same result.</li>
 * <li><b>Independence</b>: The output solely depends on the input graph. No external API calls are made (including the network and the local file system).</li>
 * <li><b>Purity</b>: The validator script does not modify any state, neither in-memory nor on disk or over network. Validator scripts are <b>not</b> allowed
 * to perform any modifications on the graph. They will receive an unmodifiable version of the graph elements, and any attempt to modify them will result in an immediate exception.</li>
 * </ul>
 *
 * <h2>Things to avoid when writing Validator Scripts</h2>
 * <ul>
 * <li><b>Do not perform excessive querying on the graph.</b> This will lead to poor performance. Checking the element and the immediate neighbors is fine.</li>
 * <li><b>Be aware of {@link NullPointerException}s and absent {@link Property properties} and {@link Edge edges}.</b> Validator scripts should be safe in this regard.</li>
 * <li><b>Be aware of unexpected {@link Property property} values.</b> Validators should not fail with {@link ClassCastException}s.</li>
 * <li><b>Do not access the file system.</b></li>
 * <li><b>Do not access the network.</b></li>
 * <li><b>Do not declare any static members, or access any non-constant static members.</b></li>
 * <li><b>Do not attempt to access any classes which are not part of ChronoGraph (i.e. do not access your own classes).</b> This will cause the validator to fail if it is invoked from outside your application (e.g. when a command-line interface is used).</li>
 * <li><b>Do not attempt to use system classes.</b> Examples include {@link System}, {@link Thread} and {@link Runtime}. Validator execution is not sandboxed!</li>
 * <li><b>Do not start threads or executor pools.</b></li>
 * <li><b>Do not open any resources (JDBC connections, file handles, input/output streams...).</b></li>
 * </ul>
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@SuppressWarnings("unused")
public interface ChronoGraphSchemaManager {

    /**
     * Adds the given validator class and associates it with the given name.
     *
     * <p>
     * In case that there is already a validator with the given name, the old validator will
     * be replaced.
     * </p>
     *
     * @param validatorName The unique name of the validator. Must not be <code>null</code> or empty.
     * @param scriptContent The Groovy script which acts as the validator body. Please see the documentation of {@link ChronoGraphSchemaManager} for the scripting API details. The script content will be compiled immediately; in case that the compilation fails, an {@link IllegalArgumentException} will be thrown and the script will <b>not</b> be stored.
     * @return <code>true</code> if a validator was overwritten, <code>false</code> if no validator was previously bound to the given name.
     */
    public boolean addOrOverrideValidator(String validatorName, String scriptContent);

    /**
     * Adds the given validator class and associates it with the given name.
     *
     * <p>
     * In case that there is already a validator with the given name, the old validator will
     * be replaced.
     * </p>
     *
     * @param validatorName The unique name of the validator. Must not be <code>null</code> or empty.
     * @param scriptContent The Groovy script which acts as the validator body. Please see the documentation of {@link ChronoGraphSchemaManager} for the scripting API details. The script content will be compiled immediately; in case that the compilation fails, an {@link IllegalArgumentException} will be thrown and the script will <b>not</b> be stored.
     * @param commitMetadata The metadata for the commit of adding or overriding a validator. May be <code>null</code>.
     * @return <code>true</code> if a validator was overwritten, <code>false</code> if no validator was previously bound to the given name.
     */
    public boolean addOrOverrideValidator(String validatorName, String scriptContent, Object commitMetadata);

    /**
     * Removes the validator with the given name.
     *
     * @param validatorName The name of the validator to remove.
     * @return <code>true</code> if the validator was successfully removed, or <code>false</code> if no validator existed with the given name.
     */
    public boolean removeValidator(String validatorName);

    /**
     * Removes the validator with the given name.
     *
     * @param validatorName The name of the validator to remove.
     * @param commitMetadata The metadata for the commit of removing a validator. May be <code>null</code>.
     * @return <code>true</code> if the validator was successfully removed, or <code>false</code> if no validator existed with the given name.
     */
    public boolean removeValidator(String validatorName, Object commitMetadata);

    /**
     * Returns the script content of the validator with the given name.
     *
     * @param validatorName The name of the validator to get the script content for. Must not be <code>null</code>.
     * @return The validator script, or <code>null</code> if there is no validator with the given name.
     */
    public String getValidatorScript(String validatorName);

    /**
     * Returns an immutable set containing all validator names that are currently in use (i.e. bound to a validator script).
     *
     * @return The set of all validator names which are currently bound to a validator script. May be empty, but never <code>null</code>.
     */
    public Set<String> getAllValidatorNames();

    /**
     * Validates the given element.
     *
     * @param branch   The branch on which the validation occurs. Must not be <code>null</code>.
     * @param elements The elements to validate. Must not be <code>null</code>.
     * @return The schema validation result. Never <code>null</code>.
     */
    public SchemaValidationResult validate(String branch, Iterable<? extends ChronoElement> elements);
}
