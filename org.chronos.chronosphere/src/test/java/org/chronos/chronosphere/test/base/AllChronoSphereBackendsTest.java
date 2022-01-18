package org.chronos.chronosphere.test.base;

import org.apache.commons.configuration2.Configuration;
import org.chronos.chronodb.test.base.AllBackendsTest;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.*;

public abstract class AllChronoSphereBackendsTest extends AllBackendsTest {

	private static final Logger log = LoggerFactory.getLogger(AllChronoSphereBackendsTest.class);

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
		log.debug("Closing ChronoSphere on backend '" + this.backend + "'.");
		if (this.chronoSphere != null && this.chronoSphere.isOpen()) {
			this.chronoSphere.close();
		}
	}

	// =====================================================================================================================
	// UTILITY
	// =====================================================================================================================

	protected ChronoSphereInternal reinstantiateChronoSphere() {
		log.debug("Reinstantiating ChronoSphere on backend '" + this.backend + "'.");
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
