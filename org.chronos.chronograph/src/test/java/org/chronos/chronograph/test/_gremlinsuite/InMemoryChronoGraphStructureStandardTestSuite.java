package org.chronos.chronograph.test._gremlinsuite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.junit.runner.RunWith;

// note: this class is NOT annotated with @Category(Suite.class) because, even though it technically is a suite,
// we do want Gradle to execute this class (whereas execution of other suites is always redundant). The reason
// is that the tests executed by this suite reside in a JAR file which is not scanned by gradle for test classes.

// This test suite is partially incompatible with Java 17+. To ensure compatibility, please add the following JVM options:
//
//    --add-opens=java.base/java.io=ALL-UNNAMED
//    --add-opens=java.base/java.nio=ALL-UNNAMED
//    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
//    --add-opens=java.base/java.lang=ALL-UNNAMED
//    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
//    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
//    --add-opens=java.base/java.util=ALL-UNNAMED
//    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
//    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
//    --add-opens=java.base/java.net=ALL-UNNAMED
//
// See: https://tinkerpop.apache.org/docs/current/upgrade/#_building_and_running_with_jdk_17
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = InMemoryChronoGraphProvider.class, graph = ChronoGraph.class)
public class InMemoryChronoGraphStructureStandardTestSuite {

}
