package org.chronos.chronograph.api.iterators.states;

import org.apache.tinkerpop.gremlin.structure.Element;

public interface AllElementsState extends GraphIteratorState {

    public Element getCurrentElement();

}
