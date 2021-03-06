package org.chronos.chronodb.api.indexing;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDB;

/**
 * A {@link Indexer} is an object capable of producing an index value for a given object.
 *
 * <p>
 * Chrono Indexers are executed every time an object is saved in {@link ChronoDB} to produce the corresponding index entries.
 *
 * <p>
 * API users are free to implement their own indexer classes. However, a number of conditions apply.
 * <ul>
 * <li>All methods of this class must be <b>idempotent</b>, i.e. consistently return the same result on the same input.
 * <li>Instances of implementing classes must be <b>stateless</b> and in particular must <b>not</b> store references to other objects, unless they are "owned" by the indexer and not accessed anywhere else.
 * <li>Implementing classes must be serializable. ChronoDB will persist them and re-load them as required.
 * <li><b>Do not implement this interface directly!</b> Implement the sub-interfaces, e.g. {@link StringIndexer} or {@link LongIndexer} instead.
 * </ul>
 *
 * <p>
 * As a general best practice, consider implementing this interface with classes that <b>have no fields</b> and do not access any static fields and/or methods.
 *
 * <p>
 * For indexers that do have states (i.e. fields), the corresponding classes need to have a proper implementation of {@link #hashCode()} and {@link #equals(Object)}!
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 * @param <T>
 *            The return type of the {@link #getIndexValues(Object)} method, i.e. the type of index (string, long...) described by this indexer.
 */
public interface Indexer<T> {

	/**
	 * Checks if this indexer is capable of processing (i.e. producing an indexed value for) the given object.
	 *
	 * <p>
	 * This method is guaranteed to be always invoked by {@link ChronoDB} before {@link #getIndexValues(Object)} is called. If this method returns <code>false</code>, then the framework assumes that this indexer is incapable of processing this object and this indexer will be ignored for this object.
	 *
	 * @param object
	 *            The object which should be indexed. Must not be <code>null</code>.
	 * @return <code>true</code> if this indexer can produce an indexed value for the given object, or <code>false</code> if the given object cannot be processed by this indexer.
	 */
	public boolean canIndex(Object object);

	/**
	 * Produces the indexed values of the given object.
	 *
	 * <p>
	 * This method is only called by {@link ChronoDB} if a previous call to {@link #canIndex(Object)} with the same parameter returned <code>true</code>.
	 *
	 * @param object
	 *            The object to index. Must not be <code>null</code>.
	 * @return A {@link Set} of elements representing the indexed values for the given object. <code>null</code>-values in the set will be ignored. Subclasses may specify more rigid constraints on this set; please check their documentation. If this method returns <code>null</code>, it will be treated as if it had returned an empty set.
	 */
	public Set<T> getIndexValues(Object object);

}
