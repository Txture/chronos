package org.chronos.common.logging;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class ChronosLogMarker {

    public static final Marker CHRONOS_LOG_MARKER__PERFORMANCE = MarkerFactory.getMarker("org.chronos.logmarker.performance");
    public static final Marker CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS = MarkerFactory.getMarker("org.chronos.logmarker.graphmodifications");


}
