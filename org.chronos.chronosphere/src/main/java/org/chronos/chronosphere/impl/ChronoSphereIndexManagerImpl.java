package org.chronos.chronosphere.impl;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronosphere.api.ChronoSphereIndexManager;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.SphereBranch;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.internal.api.ChronoSphereTransactionInternal;
import org.chronos.chronosphere.internal.ogm.api.ChronoEPackageRegistry;
import org.chronos.chronosphere.internal.ogm.api.ChronoSphereGraphFormat;
import org.eclipse.emf.ecore.EAttribute;

import java.util.Set;

public class ChronoSphereIndexManagerImpl implements ChronoSphereIndexManager {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final ChronoSphereInternal owningSphere;
	private final SphereBranch owningBranch;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public ChronoSphereIndexManagerImpl(final ChronoSphereInternal owningSphere, final SphereBranch owningBranch) {
		checkNotNull(owningSphere, "Precondition violation - argument 'owningSphere' must not be NULL!");
		this.owningSphere = owningSphere;
		this.owningBranch = owningBranch;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public boolean createIndexOn(final EAttribute eAttribute) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		try (ChronoSphereTransaction tx = this.owningSphere.tx(this.owningBranch.getName())) {
			ChronoEPackageRegistry registry = ((ChronoSphereTransactionInternal) tx).getEPackageRegistry();
			String propertyKey = ChronoSphereGraphFormat.createVertexPropertyKey(registry, eAttribute);
			if (propertyKey == null) {
				throw new IllegalArgumentException("The given EAttribute '" + eAttribute.getName()
						+ "' is not part of a registered EPackage! Please register the EPackage first.");
			}
			if (this.getGraphIndexManager().isVertexPropertyIndexedAtAnyPointInTime(propertyKey)) {
				// index already exists
				return false;
			} else {
				// index does not exist, create it
				this.getGraphIndexManager().create().stringIndex().onVertexProperty(propertyKey).acrossAllTimestamps().build();
				return true;
			}
		}
	}

	@Override
	public boolean existsIndexOn(final EAttribute eAttribute) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		try (ChronoSphereTransaction tx = this.owningSphere.tx(this.owningBranch.getName())) {
			ChronoEPackageRegistry registry = ((ChronoSphereTransactionInternal) tx).getEPackageRegistry();
			String propertyKey = ChronoSphereGraphFormat.createVertexPropertyKey(registry, eAttribute);
			if (propertyKey == null) {
				throw new IllegalArgumentException("The given EAttribute '" + eAttribute.getName()
						+ "' is not part of a registered EPackage! Please register the EPackage first.");
			}
			return this.getGraphIndexManager().isVertexPropertyIndexedAtAnyPointInTime(propertyKey);
		}
	}

	@Override
	public boolean dropIndexOn(final EAttribute eAttribute) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		try (ChronoSphereTransaction tx = this.owningSphere.tx(this.owningBranch.getName())) {
			ChronoEPackageRegistry registry = ((ChronoSphereTransactionInternal) tx).getEPackageRegistry();
			String propertyKey = ChronoSphereGraphFormat.createVertexPropertyKey(registry, eAttribute);
			if (propertyKey == null) {
				throw new IllegalArgumentException("The given EAttribute '" + eAttribute.getName()
						+ "' is not part of a registered EPackage! Please register the EPackage first.");
			}
			Set<ChronoGraphIndex> indices = this.getGraphIndexManager().getVertexIndicesAtAnyPointInTime(propertyKey);
			if (indices.isEmpty()) {
				// no index existed
				return false;
			} else {
				// index exists, drop it
				for(ChronoGraphIndex graphIndex : indices){
					this.getGraphIndexManager().dropIndex(graphIndex);
				}
				return true;
			}
		}
	}

	@Override
	public void reindexAll() {
		this.getGraphIndexManager().reindexAll();
	}

	@Override
	public void reindex(final EAttribute eAttribute) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		try (ChronoSphereTransaction tx = this.owningSphere.tx(this.owningBranch.getName())) {
			ChronoEPackageRegistry registry = ((ChronoSphereTransactionInternal) tx).getEPackageRegistry();
			String propertyKey = ChronoSphereGraphFormat.createVertexPropertyKey(registry, eAttribute);
			if (propertyKey == null) {
				throw new IllegalArgumentException("The given EAttribute '" + eAttribute.getName()
						+ "' is not part of a registered EPackage! Please register the EPackage first.");
			}
			this.getGraphIndexManager().reindexAll();
		}
	}

	@Override
	public boolean isIndexDirty(final EAttribute eAttribute) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		try (ChronoSphereTransaction tx = this.owningSphere.tx(this.owningBranch.getName())) {
			ChronoEPackageRegistry registry = ((ChronoSphereTransactionInternal) tx).getEPackageRegistry();
			String propertyKey = ChronoSphereGraphFormat.createVertexPropertyKey(registry, eAttribute);
			if (propertyKey == null) {
				throw new IllegalArgumentException("The given EAttribute '" + eAttribute.getName()
						+ "' is not part of a registered EPackage! Please register the EPackage first.");
			}
			return this.getGraphIndexManager().getDirtyIndicesAtAnyPointInTime().stream().anyMatch(idx -> idx.getIndexedProperty().equals(propertyKey));
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	protected ChronoGraphIndexManager getGraphIndexManager() {
		return this.owningSphere.getRootGraph().getIndexManagerOnBranch(this.owningBranch.getName());
	}

}
