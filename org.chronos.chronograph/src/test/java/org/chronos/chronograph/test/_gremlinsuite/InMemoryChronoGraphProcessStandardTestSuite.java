package org.chronos.chronograph.test._gremlinsuite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.junit.runner.RunWith;

// note: this class is NOT annotated with @Category(Suite.class) because, even though it technically is a suite,
// we do want Gradle to execute this class (whereas execution of other suites is always redundant). The reason
// is that the tests executed by this suite reside in a JAR file which is not scanned by gradle for test classes.
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = InMemoryChronoGraphProvider.class, graph = ChronoGraph.class)
public class InMemoryChronoGraphProcessStandardTestSuite {

}
