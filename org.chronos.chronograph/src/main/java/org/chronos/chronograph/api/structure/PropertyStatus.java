package org.chronos.chronograph.api.structure;

import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * Describes the lifecycle status of a single {@link Property}.
 */
public enum PropertyStatus {

	/**
	 * The property has been newly added in this transaction.
	 *
	 * The key was unused before that.
	 */
	NEW,

	/**
	 * The property has existed before, but was removed in this transaction.
	 */
	REMOVED,

	/**
	 * The property has existed before, but was modified in this transaction.
	 */
	MODIFIED,

	/**
	 * The property exists and is unchanged with respect to the already persisted version.
	 */
	PERSISTED,

	/**
	 * Indicates that the property has never existed on the graph element.
	 *
	 * The property key is therefore "unknown to the element".
	 */
	UNKNOWN

}
