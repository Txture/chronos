package org.chronos.chronograph.internal.impl.structure.graph.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;

public class ChronoGraphGraphFeatures extends AbstractChronoGraphFeature implements Graph.Features.GraphFeatures {

	protected ChronoGraphGraphFeatures(final ChronoGraphInternal graph) {
		super(graph);
	}

	@Override
	public boolean supportsComputer() {
		// TODO Graph feature decision PENDING
		return Graph.Features.GraphFeatures.super.supportsComputer();
	}

	@Override
	public boolean supportsConcurrentAccess() {
		// note: cannot have multiple ChronoDBs on the same data source
		return false;
	}

	@Override
	public boolean supportsPersistence() {
		// persistence support depends on the backend.
		ChronoGraphInternal graph = this.getGraph();
		ChronoDB db = graph.getBackingDB();
		return db.getFeatures().isPersistent();
	}

	@Override
	public boolean supportsTransactions() {
		return true;
	}

	@Override
	public boolean supportsThreadedTransactions() {
		return true;
	}

	public boolean supportsRollover(){
		return this.getGraph().getBackingDB().getFeatures().isRolloverSupported();
	}

}