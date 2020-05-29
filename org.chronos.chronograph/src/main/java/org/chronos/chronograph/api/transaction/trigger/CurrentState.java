package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoVertex;

import java.util.Set;

public interface CurrentState extends State {

    public Set<ChronoVertex> getModifiedVertices();

    public Set<ChronoEdge> getModifiedEdges();

    public Set<String> getModifiedGraphVariables();

    public Set<String> getModifiedGraphVariables(String keyspace);

}
