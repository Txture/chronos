package org.chronos.chronograph.api.index;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.internal.impl.index.IndexType;

/**
 * Specifies metadata about an existing graph index.
 *
 * <p>
 * Instances of this class are created when a new index is specified via the fluent builder API in {@link ChronoGraphIndexManager#create()}.
 * All existing instances can be queried by using the graph index manager methods, e.g. {@link ChronoGraphIndexManager#getAllIndices()}.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoGraphIndex {

	/**
	 * The unique ID of this index.
	 *
	 * @return The unique ID. Never <code>null</code>.
	 */
	public String getId();

	/**
	 * Returns the unique ID of the parent index (i.e. the corresponding index on the parent branch), if any.
	 *
	 * @return The unique ID of the parent index, or <code>null</code> if this index has no parent.
	 */
	public String getParentIndexId();

	/**
	 * Returns the branch to which this index is bound.
	 *
	 * @return The branch name. Never <code>null</code>.
	 */
	public String getBranch();

	/**
	 * Returns the period in which this index is valid.
	 *
	 * @return The valid period. Never <code>null</code>.
	 */
	public Period getValidPeriod();

	/**
	 * Returns the name (key) of the graph element property that is being indexed.
	 *
	 * @return The name (key) of the indexed graph element property. Never <code>null</code>.
	 */
	public String getIndexedProperty();

	/**
	 * Returns the type of graph element that is being indexed ({@linkplain Vertex} or {@linkplain Edge}).
	 *
	 * @return The graph element class that is being indexed ({@linkplain Vertex} or {@linkplain Edge}). Never <code>null</code>.
	 */
	public Class<? extends Element> getIndexedElementClass();

	/**
	 * Returns the index type, i.e. the type of the actual indexed values.
	 *
	 * <p>
	 * Please refer to the documentation of the individual {@link IndexType} literals for details.
	 *
	 * @return The index type. Never <code>null</code>.
	 */
	public IndexType getIndexType();

	public boolean isDirty();
}
