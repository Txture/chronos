package org.chronos.chronosphere.test.base;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllBackendsTest;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereInMemoryBuilder;
import org.chronos.chronosphere.api.builder.repository.ChronoSpherePropertyFileBuilder;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;
import org.junit.After;

public abstract class AllChronoSphereBackendsTest extends AllBackendsTest {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private ChronoSphereInternal chronoSphere;

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	protected ChronoSphereInternal getChronoSphere() {
		if (this.chronoSphere == null) {
			this.chronoSphere = this.instantiateChronoSphere(this.backend);
		}
		return this.chronoSphere;
	}

	// =====================================================================================================================
	// JUNIT CONTROL
	// =====================================================================================================================

	@After
	public void cleanUp() {
		ChronoLogger.logDebug("Closing ChronoSphere on backend '" + this.backend + "'.");
		if (this.chronoSphere != null && this.chronoSphere.isOpen()) {
			this.chronoSphere.close();
		}
	}

	// =====================================================================================================================
	// UTILITY
	// =====================================================================================================================

	protected ChronoSphereInternal reinstantiateChronoSphere() {
		ChronoLogger.logDebug("Reinstantiating ChronoSphere on backend '" + this.backend + "'.");
		if (this.chronoSphere != null && this.chronoSphere.isOpen()) {
			this.chronoSphere.close();
		}
		this.chronoSphere = this.instantiateChronoSphere(this.backend);
		return this.chronoSphere;
	}

	protected ChronoSphereInternal instantiateChronoSphere(final String backend) {
		checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
		Configuration configuration = this.createChronosConfiguration(backend);
		ChronoSphere sphere = ChronoSphere.FACTORY.create().fromConfiguration(configuration).build();
		return (ChronoSphereInternal)sphere;
	}

}
